package main

import (
	"context"

	"github.com/Nerzal/gocloak/v13"
	"go.uber.org/zap"
	"k8s.io/utils/env"
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

	users, err := k.client.GetUsers(ctx, token, k.config.Realm, gocloak.GetUsersParams{
		BriefRepresentation: gocloak.BoolP(true),
		Q:                   &k.config.UsersFilter,
	})
	if err != nil {
		k.logger.Errorw("failed to get keycloak users", zap.Error(err), "realm", k.config.Realm)
		return nil, err
	}

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
	return sourceUsers, nil
}

func (k *Keycloak) GetGroupsWithMembers() ([]SourceGroupWithMembers, error) {
	ctx := context.Background()
	token, err := k.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	groups, err := k.client.GetGroups(ctx, token, k.config.Realm, gocloak.GetGroupsParams{
		Search: &k.config.GroupsFilter,
	})
	if err != nil {
		k.logger.Errorw("failed to get keycloak groups", zap.Error(err), "realm", k.config.Realm)
		return nil, err
	}
	groups = k.flattenGroups(groups)

	var sourceGroups []SourceGroupWithMembers
	for _, g := range groups {
		groupName := gocloak.PString(g.Name)
		groupID := gocloak.PString(g.ID)

		members, err := k.client.GetGroupMembers(ctx, token, k.config.Realm, groupID, gocloak.GetGroupsParams{
			BriefRepresentation: gocloak.BoolP(true),
			Q:                   &k.config.UsersFilter,
		})
		if err != nil {
			k.logger.Errorw("failed to get members for keycloak group", zap.Error(err), "group", groupName)
			continue
		}

		memberIDs := NewStringSet()
		for _, m := range members {
			memberIDs.Add(gocloak.PString(m.ID))
		}

		for _, subGroup := range *g.SubGroups {
			memberIDs.Add(gocloak.PString(subGroup.ID))
		}

		sourceGroups = append(sourceGroups, SourceGroupWithMembers{
			SourceGroup: KeycloakGroup{
				Name: groupName,
				ID:   groupID,
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
