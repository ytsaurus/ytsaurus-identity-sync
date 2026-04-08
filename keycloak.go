package main

import (
	"context"

	"github.com/Nerzal/gocloak/v13"
	"go.uber.org/zap"
	"k8s.io/utils/env"
)

const (
	defaultKeycloakPageSize = 100
)

type Keycloak struct {
	client *gocloak.GoCloak
	config *KeycloakConfig
	logger appLoggerType
}

func NewKeycloak(cfg *KeycloakConfig, logger appLoggerType) (*Keycloak, error) {
	client := gocloak.NewClient(cfg.URL)

	return &Keycloak{
		client: client,
		config: cfg,
		logger: logger,
	}, nil
}

func (k *Keycloak) getAccessToken(ctx context.Context) (string, error) {
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

	var sourceUsers []SourceUser
	first := 0

	for {
		usersChunk, err := k.client.GetUsers(ctx, token, k.config.Realm, gocloak.GetUsersParams{
			BriefRepresentation: gocloak.BoolP(true),
			Q:                   &k.config.UsersFilter,
			First:               gocloak.IntP(first),
			Max:                 gocloak.IntP(defaultKeycloakPageSize),
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

	return sourceUsers, nil
}

func (k *Keycloak) GetGroupsWithMembers() ([]SourceGroupWithMembers, error) {
	ctx := context.Background()
	token, err := k.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	var sourceGroups []SourceGroupWithMembers
	first := 0

	for {
		groupsChunk, err := k.client.GetGroups(ctx, token, k.config.Realm, gocloak.GetGroupsParams{
			Search: &k.config.GroupsFilter,
			First:  gocloak.IntP(first),
			Max:    gocloak.IntP(defaultKeycloakPageSize),
		})
		if err != nil {
			k.logger.Errorw("failed to get keycloak groups", zap.Error(err), "realm", k.config.Realm)
			return nil, err
		}

		sourceGroupsChunk, err := k.convertToSourceGroups(token, groupsChunk)
		if err != nil {
			return nil, err
		}
		sourceGroups = append(sourceGroups, sourceGroupsChunk...)

		if len(groupsChunk) < defaultKeycloakPageSize {
			break
		}
		first += defaultKeycloakPageSize
	}

	return sourceGroups, nil
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
	flattenGroupsChunk := k.flattenGroups(groups)

	for _, g := range flattenGroupsChunk {
		memberIDs, err := k.getGroupMembers(token, g)
		if err != nil {
			k.logger.Errorw("failed to get group members", zap.Error(err), "group_name", gocloak.PString(g.Name))
			return nil, err
		}

		sourceGroups = append(sourceGroups, SourceGroupWithMembers{
			SourceGroup: KeycloakGroup{
				Name: gocloak.PString(g.Name),
				ID:   gocloak.PString(g.ID),
			},
			Members: memberIDs,
		})
	}

	return sourceGroups, nil
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

func (k *Keycloak) getGroupMembers(token string, group *gocloak.Group) (StringSet, error) {
	ctx := context.Background()

	memberIDs := NewStringSet()
	first := 0

	for {
		membersChunk, err := k.client.GetGroupMembers(ctx, token, k.config.Realm, *group.ID, gocloak.GetGroupsParams{
			BriefRepresentation: gocloak.BoolP(true),
			Q:                   &k.config.UsersFilter,
			First:               gocloak.IntP(first),
			Max:                 gocloak.IntP(defaultKeycloakPageSize),
		})
		if err != nil {
			return NewStringSet(), err
		}

		for _, m := range membersChunk {
			memberIDs.Add(gocloak.PString(m.ID))
		}

		if len(membersChunk) < defaultKeycloakPageSize {
			break
		}
		first += defaultKeycloakPageSize
	}

	if group.SubGroups != nil {
		for _, subGroup := range *group.SubGroups {
			memberIDs.Add(gocloak.PString(subGroup.ID))
		}
	}

	return memberIDs, nil
}
