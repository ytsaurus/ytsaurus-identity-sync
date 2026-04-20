package main

import (
	"context"
	"regexp"
	"strings"

	"github.com/Nerzal/gocloak/v13"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"k8s.io/utils/env"
)

const (
	defaultKeycloakPageSize = 100
)

type Keycloak struct {
	client           *gocloak.GoCloak
	config           *KeycloakConfig
	usersGroupFilter *regexp.Regexp
	groupsFilter     *regexp.Regexp
	logger           appLoggerType
}

type AttributeFilter struct {
	Username   string
	Email      string
	FirstName  string
	LastName   string
	Attributes map[string]string
}

func NewKeycloak(cfg *KeycloakConfig, logger appLoggerType) (*Keycloak, error) {
	client := gocloak.NewClient(cfg.URL)

	usersGroupfilter, err := regexp.Compile(cfg.UsersGroupFilter)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse users group filter")
	}
	groupsFilter, err := regexp.Compile(cfg.GroupsFilter)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse groups filter")
	}

	return &Keycloak{
		client:           client,
		config:           cfg,
		usersGroupFilter: usersGroupfilter,
		groupsFilter:     groupsFilter,
		logger:           logger,
	}, nil
}

func (k *Keycloak) CreateUserFromRaw(raw map[string]any) (SourceUser, error) {
	return NewKeycloakUser(raw)
}

func (k *Keycloak) CreateGroupFromRaw(raw map[string]any) (SourceGroup, error) {
	return NewKeycloakGroup(raw)
}

func (k *Keycloak) GetUsers() ([]SourceUser, error) {
	ctx := context.Background()
	token, err := k.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	if k.config.UsersGroupFilter != "" {
		return k.GetUsersByGroups(token)
	}

	return k.GetUsersByAttributes(token)
}

func (k *Keycloak) GetGroupsWithMembers() ([]SourceGroupWithMembers, error) {
	ctx := context.Background()
	token, err := k.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	keycloakGroups, err := k.getGroupsByRegexp(token, k.groupsFilter)
	if err != nil {
		return nil, err
	}

	k.logger.Debugf("Found %d groups", len(keycloakGroups))

	return k.convertToSourceGroups(token, keycloakGroups)
}

func (k *Keycloak) getAccessToken(ctx context.Context) (string, error) {
	k.logger.Debugf("Getting access token for client %s", k.config.ClientID)
	token, err := k.client.LoginClient(
		ctx,
		k.config.ClientID,
		env.GetString(k.config.ClientSecretEnvVar, ""),
		k.config.Realm,
	)
	if err != nil {
		k.logger.Errorw("failed to get keycloak client token", zap.Error(err), "client_id", k.config.ClientID)
		return "", err
	}
	return token.AccessToken, nil
}

func (k *Keycloak) GetUsersByAttributes(token string) ([]SourceUser, error) {
	ctx := context.Background()

	var sourceUsers []SourceUser
	first := 0

	for {
		k.logger.Debugf("Processing users from %d ...", first)
		usersChunk, err := k.client.GetUsers(ctx, token, k.config.Realm, gocloak.GetUsersParams{
			BriefRepresentation: gocloak.BoolP(true),
			Q:                   &k.config.UsersAttributeFilter,
			First:               gocloak.IntP(first),
			Max:                 gocloak.IntP(defaultKeycloakPageSize),
			EmailVerified:       gocloak.BoolP(true),
			Enabled:             gocloak.BoolP(true),
		})
		if err != nil {
			k.logger.Errorw("failed to get keycloak users", zap.Error(err), "realm", k.config.Realm)
			return nil, err
		}

		sourceUsers = append(sourceUsers, k.convertToSourceUsers(usersChunk)...)

		if len(usersChunk) < defaultKeycloakPageSize {
			break
		}

		first += defaultKeycloakPageSize
	}

	k.logger.Debugf("Found %d users", len(sourceUsers))

	return sourceUsers, nil
}

func (k *Keycloak) GetUsersByGroups(token string) ([]SourceUser, error) {
	groups, err := k.getGroupsByRegexp(token, k.usersGroupFilter)
	if err != nil {
		return nil, err
	}

	var sourceUsers []SourceUser
	userIDsMap := make(map[ObjectID]bool)
	for _, g := range groups {
		members, err := k.getGroupMembers(token, g)
		if err != nil {
			return nil, err
		}

		members = k.filterUsersByAttributes(members, k.config.UsersAttributeFilter)
		convertedMembers := k.convertToSourceUsers(members)

		for _, m := range convertedMembers {
			if !userIDsMap[m.GetID()] {
				userIDsMap[m.GetID()] = true
				sourceUsers = append(sourceUsers, m)
			}
		}
	}

	k.logger.Debugf("Found %d users", len(sourceUsers))

	return sourceUsers, nil
}

func (k *Keycloak) getGroupsByRegexp(token string, filter *regexp.Regexp) ([]*gocloak.Group, error) {
	ctx := context.Background()

	var groups []*gocloak.Group
	first := 0

	for {
		k.logger.Debugf("Processing groups from %d ...", first)
		groupsChunk, err := k.client.GetGroups(ctx, token, k.config.Realm, gocloak.GetGroupsParams{
			Search: gocloak.StringP(""),
			First:  gocloak.IntP(first),
			Max:    gocloak.IntP(defaultKeycloakPageSize),
		})
		if err != nil {
			k.logger.Errorw("failed to get keycloak groups", zap.Error(err), "realm", k.config.Realm)
			return nil, err
		}

		flattenGroupsChunk := k.flattenGroups(groupsChunk)
		flattenGroupsChunk = k.filterGroups(flattenGroupsChunk, filter)
		groups = append(groups, flattenGroupsChunk...)

		if len(groupsChunk) < defaultKeycloakPageSize {
			break
		}
		first += defaultKeycloakPageSize
	}

	return groups, nil
}

func (k *Keycloak) filterGroups(groups []*gocloak.Group, filter *regexp.Regexp) []*gocloak.Group {
	var filteredGroups []*gocloak.Group

	for _, g := range groups {
		if filter.MatchString(gocloak.PString(g.Name)) {
			filteredGroups = append(filteredGroups, g)
		}
	}
	return filteredGroups
}

func (k *Keycloak) filterUsersByAttributes(users []*gocloak.User, filter string) []*gocloak.User {
	attributesFilter := parseAttributesFilter(filter)
	var filteredUsers []*gocloak.User
	for _, u := range users {
		if isUserMatchFilter(u, attributesFilter) {
			filteredUsers = append(filteredUsers, u)
		}
	}
	return filteredUsers
}

func (k *Keycloak) flattenGroups(groups []*gocloak.Group) []*gocloak.Group {
	var flatList []*gocloak.Group
	flatList = append(flatList, groups...)

	for i := 0; i < len(flatList); i++ {
		group := flatList[i]

		if group.SubGroups == nil {
			continue
		}

		for _, subGroup := range *group.SubGroups {
			flatList = append(flatList, &subGroup)
		}
	}

	return flatList
}

func (k *Keycloak) getGroupMembers(token string, group *gocloak.Group) ([]*gocloak.User, error) {
	ctx := context.Background()

	var members []*gocloak.User
	first := 0

	for {
		k.logger.Debugf("Processing group %s members from %d ...", gocloak.PString(group.Name), first)
		membersChunk, err := k.client.GetGroupMembers(ctx, token, k.config.Realm, *group.ID, gocloak.GetGroupsParams{
			First: gocloak.IntP(first),
			Max:   gocloak.IntP(defaultKeycloakPageSize),
		})
		if err != nil {
			return nil, err
		}

		for _, m := range membersChunk {
			if gocloak.PBool(m.EmailVerified) && gocloak.PBool(m.Enabled) {
				members = append(members, m)
			}
		}

		if len(membersChunk) < defaultKeycloakPageSize {
			break
		}
		first += defaultKeycloakPageSize
	}

	k.logger.Debugf("Found %d group %s members", len(members), gocloak.PString(group.Name))

	return members, nil
}

func (k *Keycloak) convertToSourceUsers(users []*gocloak.User) []SourceUser {
	var sourceUsers []SourceUser
	for _, u := range users {
		sourceUsers = append(sourceUsers, KeycloakUser{
			Username:  gocloak.PString(u.Username),
			ID:        gocloak.PString(u.ID),
			FirstName: gocloak.PString(u.FirstName),
			LastName:  gocloak.PString(u.LastName),
			Email:     gocloak.PString(u.Email),
		})
	}
	return sourceUsers
}

func (k *Keycloak) convertToSourceGroups(token string, groups []*gocloak.Group) ([]SourceGroupWithMembers, error) {
	var sourceGroups []SourceGroupWithMembers

	for _, g := range groups {
		memberIDs, err := k.getGroupMemberIDs(token, g)
		if err != nil {
			k.logger.Errorw("failed to get group members", zap.Error(err), "group_name", gocloak.PString(g.Name))
			return nil, err
		}

		subGroupIDs := NewStringSet()
		if g.SubGroups != nil {
			for _, subGroup := range *g.SubGroups {
				subGroupIDs.Add(gocloak.PString(subGroup.ID))
			}
		}

		sourceGroups = append(sourceGroups, SourceGroupWithMembers{
			SourceGroup: KeycloakGroup{
				Name: gocloak.PString(g.Name),
				ID:   gocloak.PString(g.ID),
			},
			Members:   memberIDs,
			SubGroups: subGroupIDs,
		})
	}

	return sourceGroups, nil
}

func (k *Keycloak) getGroupMemberIDs(token string, group *gocloak.Group) (StringSet, error) {
	memberIDs := NewStringSet()

	userMembers, err := k.getGroupMembers(token, group)
	if err != nil {
		return nil, err
	}

	for _, u := range userMembers {
		memberIDs.Add(gocloak.PString(u.ID))
	}

	return memberIDs, nil
}

func parseAttributesFilter(filter string) AttributeFilter {
	attributeFilter := AttributeFilter{
		Attributes: make(map[string]string),
	}
	if filter == "" {
		return attributeFilter
	}

	for _, f := range strings.Split(filter, " ") {
		if !strings.Contains(f, ":") {
			continue
		}
		kv := strings.Split(f, ":")
		switch kv[0] {
		case "username":
			attributeFilter.Username = kv[1]
		case "email":
			attributeFilter.Email = kv[1]
		case "firstName":
			attributeFilter.FirstName = kv[1]
		case "lastName":
			attributeFilter.LastName = kv[1]
		default:
			attributeFilter.Attributes[kv[0]] = kv[1]
		}
	}

	return attributeFilter
}

func isUserMatchFilter(user *gocloak.User, filter AttributeFilter) bool {
	match := strings.Contains(gocloak.PString(user.Username), filter.Username) &&
		strings.Contains(gocloak.PString(user.Email), filter.Email) &&
		strings.Contains(gocloak.PString(user.FirstName), filter.FirstName) &&
		strings.Contains(gocloak.PString(user.LastName), filter.LastName)

	if !match {
		return false
	}

	userAttributes := make(map[string][]string)
	if user.Attributes != nil {
		userAttributes = *user.Attributes
	}

	for k, v := range filter.Attributes {
		if _, ok := userAttributes[k]; !ok {
			return false
		}
		for _, u := range userAttributes[k] {
			match = match && strings.Contains(u, v)
		}
	}

	return match
}
