package main

import (
	"context"
	"errors"
	"fmt"
	"net"

	keycloak "github.com/stillya/testcontainers-keycloak"
	"github.com/testcontainers/testcontainers-go"
)

type KeycloakLocal struct {
	container testcontainers.Container
}

func NewKeycloakLocal() *KeycloakLocal {
	return &KeycloakLocal{}
}

func (k *KeycloakLocal) Start() error {
	container, err := keycloak.Run(context.Background(), "quay.io/keycloak/keycloak:26.0.7",
		keycloak.WithAdminUsername("admin"),
		keycloak.WithAdminPassword("admin"),
	)
	if err != nil {
		return err
	}

	k.container = container
	return nil
}

func (k *KeycloakLocal) GetConfig() (*KeycloakConfig, error) {
	ctx := context.Background()
	host, err := k.container.Host(ctx)
	if err != nil {
		return nil, err
	}

	containerPort, err := k.container.MappedPort(ctx, "8080/tcp")
	if err != nil {
		return nil, err
	}

	return &KeycloakConfig{
		URL:                fmt.Sprintf("http://%s", net.JoinHostPort(host, containerPort.Port())),
		Realm:              "test-realm",
		ClientID:           "test-client",
		ClientSecretEnvVar: "KEYCLOAK_CLIENT_SECRET",
	}, nil
}

func (k *KeycloakLocal) Stop() error {
	if k.container == nil {
		return errors.New("container not started")
	}
	return k.container.Terminate(context.Background())
}
