package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Nerzal/gocloak/v13"
	"github.com/stretchr/testify/require"
	testclock "k8s.io/utils/clock/testing"
)

func fullUsername(name string) string {
	return fmt.Sprintf("%s@acme.com", name)
}

func createKeycloakUser(name string) KeycloakUser {
	return KeycloakUser{
		Username:  fullUsername(name),
		FirstName: name,
		LastName:  name + "-surname",
		Email:     name + "@acme.com",
	}
}

func createUpdatedKeycloakUser(name string) KeycloakUser {
	user := createKeycloakUser(name)
	return KeycloakUser{
		Username:  user.Username,
		FirstName: user.FirstName + "-updated",
		LastName:  name + "-surname",
		Email:     name + "@acme.com",
	}
}

func fullGroupName(name string) string {
	return fmt.Sprintf("acme.%s|all", name)
}

func createKeycloakGroup(name string) KeycloakGroup {
	return KeycloakGroup{
		Name: fullGroupName(name),
	}
}

func createYtsaurusUserForKeycloak(name string) YtsaurusUser {
	originalUsername := fullUsername(name)
	ytUsername := originalUsername
	for _, replacement := range defaultUsernameReplacements {
		ytUsername = strings.Replace(ytUsername, replacement.From, replacement.To, -1)
	}
	return YtsaurusUser{Username: ytUsername, SourceRaw: map[string]any{
		"username":   originalUsername,
		"id":         "user_id_stub_" + originalUsername,
		"first_name": name,
		"last_name":  name + "-surname",
		"email":      name + "@acme.com",
	}}
}

func createUpdatedYtsaurusUserForKeycloak(name string) YtsaurusUser {
	user := createYtsaurusUserForKeycloak(name)
	user.SourceRaw["first_name"] = name + "-updated"
	return user
}

func createYtsaurusGroupForKeycloak(name string) YtsaurusGroup {
	originalName := fullGroupName(name)
	ytName := originalName
	for _, replacement := range defaultGroupnameReplacements {
		ytName = strings.Replace(ytName, replacement.From, replacement.To, -1)
	}
	return YtsaurusGroup{Name: ytName, SourceRaw: map[string]any{
		"name": originalName,
		"id":   "group_id_stub_" + originalName,
	}}
}

var (
	keycloakTestCases = []testCase{
		{
			name: "a-skip-b-create-c-remove",
			sourceUsersSetUp: []SourceUser{
				createKeycloakUser(aliceName),
				createKeycloakUser(bobName),
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(carolName),
			},
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(bobName),
			},
		},
		{
			name: "bob-is-banned",
			appConfig: &AppConfig{
				UsernameReplacements:    defaultUsernameReplacements,
				GroupnameReplacements:   defaultGroupnameReplacements,
				BanBeforeRemoveDuration: 24 * time.Hour,
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(bobName),
			},
			sourceUsersSetUp: []SourceUser{
				createKeycloakUser(aliceName),
			},
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				bannedYtsaurusUser(createYtsaurusUserForKeycloak(bobName), initialTestTime),
			},
		},
		{
			name: "bob-was-banned-now-deleted-carol-was-banned-now-back",
			// Bob was banned at initialTestTime,
			// 2 days have passed (more than setting allows) —> he should be removed.
			// Carol was banned 8 hours ago and has been found in Azure -> she should be restored.
			testTime: initialTestTime.Add(48 * time.Hour),
			appConfig: &AppConfig{
				UsernameReplacements:    defaultUsernameReplacements,
				GroupnameReplacements:   defaultGroupnameReplacements,
				BanBeforeRemoveDuration: 24 * time.Hour,
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				bannedYtsaurusUser(createYtsaurusUserForKeycloak(bobName), initialTestTime),
				bannedYtsaurusUser(createYtsaurusUserForKeycloak(carolName), initialTestTime.Add(40*time.Hour)),
			},
			sourceUsersSetUp: []SourceUser{
				createKeycloakUser(aliceName),
				createKeycloakUser(carolName),
			},
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(carolName),
			},
		},
		{
			name: "remove-limit-users-3",
			appConfig: &AppConfig{
				UsernameReplacements:  defaultUsernameReplacements,
				GroupnameReplacements: defaultGroupnameReplacements,
				RemoveLimit:           3,
			},
			sourceUsersSetUp: []SourceUser{},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(bobName),
				createYtsaurusUserForKeycloak(carolName),
			},
			// No one is deleted: limitation works.
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(bobName),
				createYtsaurusUserForKeycloak(carolName),
			},
		},
		{
			name: "remove-limit-groups-3",
			appConfig: &AppConfig{
				UsernameReplacements:  defaultUsernameReplacements,
				GroupnameReplacements: defaultGroupnameReplacements,
				RemoveLimit:           3,
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroupForKeycloak("devs")),
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroupForKeycloak("qa")),
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroupForKeycloak("hq")),
			},
			// No group is deleted: limitation works.
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroupForKeycloak("devs")),
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroupForKeycloak("qa")),
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroupForKeycloak("hq")),
			},
		},
		{
			name: "a-changed-name-b-changed-email",
			sourceUsersSetUp: []SourceUser{
				createUpdatedKeycloakUser(aliceName),
				createUpdatedKeycloakUser(bobName),
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(bobName),
			},
			ytUsersExpected: []YtsaurusUser{
				createUpdatedYtsaurusUserForKeycloak(aliceName),
				createUpdatedYtsaurusUserForKeycloak(bobName),
			},
		},
		{
			name: "skip-create-remove-group-no-members-change-correct-name-replace",
			sourceUsersSetUp: []SourceUser{
				createKeycloakUser(aliceName),
				createKeycloakUser(bobName),
				createKeycloakUser(carolName),
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(bobName),
				createYtsaurusUserForKeycloak(carolName),
			},
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(bobName),
				createYtsaurusUserForKeycloak(carolName),
			},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs"),
					Members:       NewStringSetFromItems(aliceName),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("qa"),
					Members:       NewStringSetFromItems(bobName),
				},
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					SourceGroup: createKeycloakGroup("devs"),
					Members:     NewStringSetFromItems(fullUsername(aliceName)),
				},
				{
					SourceGroup: createKeycloakGroup("hq"),
					Members:     NewStringSetFromItems(fullUsername(carolName)),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs"),
					Members:       NewStringSetFromItems(aliceName),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("hq"),
					Members:       NewStringSetFromItems(carolName),
				},
			},
		},
		{
			name: "memberships-add-remove",
			sourceUsersSetUp: []SourceUser{
				createKeycloakUser(aliceName),
				createKeycloakUser(bobName),
				createKeycloakUser(carolName),
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(bobName),
				createYtsaurusUserForKeycloak(carolName),
			},
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(bobName),
				createYtsaurusUserForKeycloak(carolName),
			},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs"),
					Members: NewStringSetFromItems(
						aliceName,
						bobName,
					),
				},
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					SourceGroup: createKeycloakGroup("devs"),
					Members: NewStringSetFromItems(
						fullUsername(aliceName),
						fullUsername(carolName),
					),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs"),
					Members: NewStringSetFromItems(
						aliceName,
						carolName,
					),
				},
			},
		},
		{
			name: "memberships-add-remove-subgroups",
			sourceUsersSetUp: []SourceUser{
				createKeycloakUser(aliceName),
				createKeycloakUser(bobName),
				createKeycloakUser(carolName),
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(bobName),
				createYtsaurusUserForKeycloak(carolName),
			},
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUserForKeycloak(aliceName),
				createYtsaurusUserForKeycloak(bobName),
				createYtsaurusUserForKeycloak(carolName),
			},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs-subgroup1"),
					Members: NewStringSetFromItems(
						aliceName,
					),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs-subgroup2"),
					Members: NewStringSetFromItems(
						bobName,
					),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs"),
					Members: NewStringSetFromItems(
						aliceName,
						bobName,
						"acme.devs-subgroup1",
						"acme.devs-subgroup2",
					),
				},
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					SourceGroup: createKeycloakGroup("devs-subgroup1"),
					Members: NewStringSetFromItems(
						fullUsername(aliceName),
					),
				},
				{
					SourceGroup: createKeycloakGroup("devs-subgroup3"),
					Members: NewStringSetFromItems(
						fullUsername(carolName),
					),
				},
				{
					SourceGroup: createKeycloakGroup("devs"),
					Members: NewStringSetFromItems(
						fullUsername(aliceName),
						fullUsername(carolName),
					),
					SubGroups: NewStringSetFromItems(
						fullGroupName("devs-subgroup1"),
						fullGroupName("devs-subgroup3"),
					),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs-subgroup1"),
					Members: NewStringSetFromItems(
						aliceName,
					),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs-subgroup3"),
					Members: NewStringSetFromItems(
						carolName,
					),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs"),
					Members: NewStringSetFromItems(
						aliceName,
						carolName,
						"acme.devs-subgroup1",
						"acme.devs-subgroup3",
					),
				},
			},
		},
		{
			name: "memberships-move-subgroups",
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs-subgroup1-subgroup1"),
					Members:       NewStringSetFromItems(),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs-subgroup1"),
					Members: NewStringSetFromItems(
						"acme.devs-subgroup1-subgroup1",
					),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs-subgroup2"),
					Members:       NewStringSetFromItems(),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs"),
					Members: NewStringSetFromItems(
						"acme.devs-subgroup1",
					),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("hq"),
					Members: NewStringSetFromItems(
						"acme.devs-subgroup2",
					),
				},
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					SourceGroup: createKeycloakGroup("devs-subgroup1-subgroup1"),
					Members:     NewStringSetFromItems(),
				},
				{
					SourceGroup: createKeycloakGroup("devs-subgroup1"),
					Members:     NewStringSetFromItems(),
				},
				{
					SourceGroup: createKeycloakGroup("devs-subgroup2"),
					SubGroups: NewStringSetFromItems(
						fullGroupName("devs-subgroup1-subgroup1"),
					),
				},
				{
					SourceGroup: createKeycloakGroup("devs"),
					SubGroups: NewStringSetFromItems(
						fullGroupName("devs-subgroup2"),
					),
				},
				{
					SourceGroup: createKeycloakGroup("hq"),
					SubGroups: NewStringSetFromItems(
						fullGroupName("devs-subgroup1"),
					),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs-subgroup1-subgroup1"),
					Members:       NewStringSetFromItems(),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs-subgroup1"),
					Members:       NewStringSetFromItems(),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs-subgroup2"),
					Members: NewStringSetFromItems(
						"acme.devs-subgroup1-subgroup1",
					),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs"),
					Members: NewStringSetFromItems(
						"acme.devs-subgroup2",
					),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("hq"),
					Members: NewStringSetFromItems(
						"acme.devs-subgroup1",
					),
				},
			},
		},
		{
			name: "create-users-groups-filtered",
			keycloakConfigModifier: func(cfg *KeycloakConfig) {
				cfg.UsersAttributeFilter = "username:test_ email:@acme.com"
				cfg.UsersGroupFilter = "devs"
				cfg.GroupsFilter = "de"
			},
			sourceUsersSetUp: []SourceUser{
				createKeycloakUser("test_alice"),
				createKeycloakUser("test_bob"),
				createKeycloakUser("carol"),
			},
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUserForKeycloak("test_alice"),
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					SourceGroup: createKeycloakGroup("devs"),
					Members: NewStringSetFromItems(
						fullUsername("test_alice"),
						fullUsername("carol"),
					),
				},
				{
					SourceGroup: createKeycloakGroup("defs"),
					Members: NewStringSetFromItems(
						fullUsername("test_bob"),
					),
				},
				{
					SourceGroup: createKeycloakGroup("hq"),
					Members:     NewStringSetFromItems(),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("devs"),
					Members: NewStringSetFromItems(
						"test_alice",
					),
				},
				{
					YtsaurusGroup: createYtsaurusGroupForKeycloak("defs"),
					Members:       NewStringSetFromItems(),
				},
			},
		},
		{
			name:              "create-with-paging",
			sourceUsersSetUp:  generateSourceUsers(210),
			sourceGroupsSetUp: generateEmptySourceGroups(210),
			ytUsersExpected:   generateYTUsers(210),
			ytGroupsExpected:  generateEmptyYTGroups(210),
		},
	}
)

func (suite *AppTestSuite) TestKeycloakSyncOnce() {
	t := suite.T()

	for _, tc := range keycloakTestCases {
		t.Run(
			tc.name,
			func(tc testCase) func(t *testing.T) {
				return func(t *testing.T) {
					defer suite.clear()

					if tc.testTime.IsZero() {
						tc.testTime = initialTestTime
					}
					passiveClock := testclock.NewFakePassiveClock(tc.testTime)

					keycloakLocal := NewKeycloakLocal()
					defer func() { require.NoError(t, keycloakLocal.Stop()) }()
					require.NoError(t, keycloakLocal.Start())

					kcConfig, err := keycloakLocal.GetConfig()
					require.NoError(t, err)
					if tc.keycloakConfigModifier != nil {
						tc.keycloakConfigModifier(kcConfig)
					}

					clientSecret := "test-client-secret"
					require.NoError(t, os.Setenv(kcConfig.ClientSecretEnvVar, clientSecret))
					defer os.Unsetenv(kcConfig.ClientSecretEnvVar)

					setupKeycloakObjects(t, kcConfig, clientSecret, tc.sourceUsersSetUp, tc.sourceGroupsSetUp)
					setIDsToYTsaurusObjects(tc.sourceUsersSetUp, tc.sourceGroupsSetUp, tc.ytUsersSetUp, tc.ytGroupsSetUp)
					setupYtsaurusObjects(t, suite.ytsaurusClient, tc.ytUsersSetUp, tc.ytGroupsSetUp)

					keycloakSource, err := NewKeycloak(kcConfig, getDevelopmentLogger())
					require.NoError(t, err)

					suite.syncOnce(t, keycloakSource, passiveClock, tc.appConfig)

					expectedSourceUsers, err := keycloakSource.GetUsers()
					require.NoError(t, err)
					expectedSourceGroups, err := keycloakSource.GetGroupsWithMembers()
					require.NoError(t, err)
					setIDsToYTsaurusObjects(expectedSourceUsers, expectedSourceGroups, tc.ytUsersExpected, tc.ytGroupsExpected)

					suite.check(t, tc.ytUsersExpected, tc.ytGroupsExpected)
				}
			}(tc),
		)
	}
}

func setIDsToYTsaurusObjects(sourceUsers []SourceUser, sourceGroups []SourceGroupWithMembers, ytUsers []YtsaurusUser, ytGroups []YtsaurusGroupWithMembers) {
	sourceUsersMap := make(map[string]SourceUser)
	for _, user := range sourceUsers {
		sourceUsersMap[user.GetName()] = user
	}

	sourceGroupsMap := make(map[string]SourceGroupWithMembers)
	for _, group := range sourceGroups {
		sourceGroupsMap[group.SourceGroup.GetName()] = group
	}

	for i, ytUser := range ytUsers {
		if sourceUser, ok := sourceUsersMap[ytUser.SourceRaw["username"].(string)]; ok {
			ytUserRow, _ := sourceUser.GetRaw()
			ytUser.SourceRaw = ytUserRow
			ytUsers[i] = ytUser
		}
	}

	for i, ytGroup := range ytGroups {
		if sourceGroup, ok := sourceGroupsMap[ytGroup.SourceRaw["name"].(string)]; ok {
			ytGroupRow, _ := sourceGroup.SourceGroup.GetRaw()
			ytGroup.SourceRaw = ytGroupRow
			ytGroups[i] = ytGroup
		}
	}
}

func setupKeycloakObjects(t *testing.T, cfg *KeycloakConfig, clientSecret string, users []SourceUser, groups []SourceGroupWithMembers) {
	t.Log("Setting up keycloak for test")
	ctx := context.Background()
	client := gocloak.NewClient(cfg.URL)

	token, err := client.LoginAdmin(ctx, "admin", "admin", "master")
	require.NoError(t, err)

	_, err = client.CreateRealm(ctx, token.AccessToken, gocloak.RealmRepresentation{
		Realm:   gocloak.StringP(cfg.Realm),
		Enabled: gocloak.BoolP(true),
	})
	require.NoError(t, err)

	realmClientID, err := client.CreateClient(ctx, token.AccessToken, cfg.Realm, gocloak.Client{
		ClientID:               gocloak.StringP(cfg.ClientID),
		Secret:                 gocloak.StringP(clientSecret),
		ServiceAccountsEnabled: gocloak.BoolP(true),
		Enabled:                gocloak.BoolP(true),
		PublicClient:           gocloak.BoolP(false),
	})
	require.NoError(t, err)

	saUser, err := client.GetClientServiceAccount(ctx, token.AccessToken, cfg.Realm, realmClientID)
	require.NoError(t, err)

	saUserID := *saUser.ID

	realManagementClients, err := client.GetClients(ctx, token.AccessToken, cfg.Realm, gocloak.GetClientsParams{
		ClientID: gocloak.StringP("realm-management"),
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(realManagementClients))

	realmManagementInternalID := *realManagementClients[0].ID

	queryUsersRole, err := client.GetClientRole(ctx, token.AccessToken, cfg.Realm, realmManagementInternalID, "view-users")
	require.NoError(t, err)
	queryGroupsRole, err := client.GetClientRole(ctx, token.AccessToken, cfg.Realm, realmManagementInternalID, "query-groups")
	require.NoError(t, err)

	err = client.AddClientRolesToUser(ctx, token.AccessToken, cfg.Realm, realmManagementInternalID, saUserID, []gocloak.Role{
		*queryUsersRole,
		*queryGroupsRole,
	})
	require.NoError(t, err)

	for i, user := range users {
		ku := user.(KeycloakUser)

		t.Logf("creating user: %v", user)
		id, err := client.CreateUser(ctx, token.AccessToken, cfg.Realm, gocloak.User{
			Username:  gocloak.StringP(ku.Username),
			FirstName: gocloak.StringP(ku.FirstName),
			LastName:  gocloak.StringP(ku.LastName),
			Email:     gocloak.StringP(ku.Email),
			Enabled:   gocloak.BoolP(true),
		})
		require.NoError(t, err)
		ku.ID = id
		users[i] = ku
	}

	usersMap := make(map[string]KeycloakUser)
	for _, user := range users {
		usersMap[user.GetName()] = user.(KeycloakUser)
	}

	groupIDsMap := make(map[string]string)

	for i, group := range groups {
		t.Logf("creating group: %v", group)
		kg := group.SourceGroup.(KeycloakGroup)
		groupID, err := client.CreateGroup(ctx, token.AccessToken, cfg.Realm, gocloak.Group{
			Name: gocloak.StringP(kg.Name),
		})
		require.NoError(t, err)
		kg.ID = groupID
		group.SourceGroup = kg

		groupIDsMap[kg.Name] = groupID

		groupMembers := NewStringSet()
		if group.Members == nil {
			group.Members = NewStringSet()
		}
		for member := range group.Members.Iter() {
			if user, ok := usersMap[member]; ok {
				t.Logf("adding member %s to group %s", member, group.SourceGroup.GetName())
				err = client.AddUserToGroup(ctx, token.AccessToken, cfg.Realm, user.ID, groupID)
				require.NoError(t, err)
				groupMembers.Add(user.ID)
			} else {
				require.FailNow(t, fmt.Sprintf("Unknown member name: %s", member))
			}
		}
		if group.SubGroups == nil {
			group.SubGroups = NewStringSet()
		}
		for subGroup := range group.SubGroups.Iter() {
			if subGroupID, ok := groupIDsMap[subGroup]; ok {
				t.Logf("adding subgroup %s to group %s", subGroup, group.SourceGroup.GetName())
				subGroupID, err := client.CreateChildGroup(ctx, token.AccessToken, cfg.Realm, groupID, gocloak.Group{
					ID:   gocloak.StringP(subGroupID),
					Name: gocloak.StringP(subGroup),
				})
				require.NoError(t, err)

				groupMembers.Add(subGroupID)
			} else {
				require.FailNow(t, fmt.Sprintf("Unknown subgroup name: %s", subGroup))
			}
		}
		group.Members = groupMembers
		groups[i] = group
	}
}

func generateSourceUsers(count int) []SourceUser {
	users := make([]SourceUser, count)
	for i := 0; i < count; i++ {
		users[i] = createKeycloakUser(fmt.Sprintf("user-%d", i))
	}
	return users
}

func generateYTUsers(count int) []YtsaurusUser {
	users := make([]YtsaurusUser, count)
	for i := 0; i < count; i++ {
		users[i] = createYtsaurusUserForKeycloak(fmt.Sprintf("user-%d", i))
	}
	return users
}

func generateEmptySourceGroups(count int) []SourceGroupWithMembers {
	groups := make([]SourceGroupWithMembers, count)
	for i := 0; i < count; i++ {
		groups[i] = SourceGroupWithMembers{
			SourceGroup: createKeycloakGroup(fmt.Sprintf("group-%d", i)),
			Members:     NewStringSetFromItems(),
		}
	}
	return groups
}

func generateEmptyYTGroups(count int) []YtsaurusGroupWithMembers {
	groups := make([]YtsaurusGroupWithMembers, count)
	for i := 0; i < count; i++ {
		groups[i] = YtsaurusGroupWithMembers{
			YtsaurusGroup: createYtsaurusGroupForKeycloak(fmt.Sprintf("group-%d", i)),
			Members:       NewStringSetFromItems(),
		}
	}
	return groups
}
