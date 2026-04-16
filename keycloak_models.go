package main

import "go.ytsaurus.tech/yt/go/yson"

type KeycloakUser struct {
	Username  string `yson:"username"`
	ID        string `yson:"id"`
	FirstName string `yson:"first_name"`
	LastName  string `yson:"last_name"`
	Email     string `yson:"email"`
}

func NewKeycloakUser(attributes map[string]any) (*KeycloakUser, error) {
	bytes, err := yson.Marshal(attributes)
	if err != nil {
		return nil, err
	}

	var user KeycloakUser
	err = yson.Unmarshal(bytes, &user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func (ku KeycloakUser) GetID() ObjectID {
	return ku.ID
}

func (ku KeycloakUser) GetName() string {
	return ku.Username
}

func (ku KeycloakUser) GetRaw() (map[string]any, error) {
	bytes, err := yson.Marshal(ku)
	if err != nil {
		return nil, err
	}

	raw := make(map[string]any)
	err = yson.Unmarshal(bytes, &raw)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

type KeycloakGroup struct {
	Name string `yson:"name"`
	ID   string `yson:"id"`
}

func NewKeycloakGroup(attributes map[string]any) (*KeycloakGroup, error) {
	bytes, err := yson.Marshal(attributes)
	if err != nil {
		return nil, err
	}

	var group KeycloakGroup
	err = yson.Unmarshal(bytes, &group)
	if err != nil {
		return nil, err
	}
	return &group, nil
}

func (kg KeycloakGroup) GetID() ObjectID {
	return kg.ID
}

func (kg KeycloakGroup) GetName() string {
	return kg.Name
}

func (kg KeycloakGroup) GetRaw() (map[string]any, error) {
	bytes, err := yson.Marshal(kg)
	if err != nil {
		return nil, err
	}

	raw := make(map[string]any)
	err = yson.Unmarshal(bytes, &raw)
	if err != nil {
		return nil, err
	}
	return raw, nil
}
