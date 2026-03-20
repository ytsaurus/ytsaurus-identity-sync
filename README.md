# ytsaurus-identity-sync

## Installing
To install application to your k8s cluster — use helm chart with something like that:
```
helm upgrade --install --wait \
 -f ytsaurus-identity-sync.values.yaml \
 -n idsync --create-namespace \
 idsync oci://ghcr.io/ytsaurus/ytsaurus-identity-sync-chart \
--version 0.0.2
```
Examples for helm values can be found in the [examples](examples) directory.  
All configuration options for app can be found in [main/config.go](main/config.go) file.


## Official release
To issue an official release of app — create new release at the [releases](https://github.com/ytsaurus/ytsaurus-identity-sync/releases) tab with some release notes.  
For the release create a tag matching pattern `release/X.X.X`.  
Images will be build automatically on release tag creation.

## Development releases
Application docker image and helm oci images are created on each commit to the main branch and uploaded to Github Packages.  
[app registry](https://github.com/ytsaurus/ytsaurus-identity-sync/pkgs/container/ytsaurus-identity-sync)  
[chart registry](https://github.com/ytsaurus/ytsaurus-identity-sync/pkgs/container/ytsaurus-identity-sync-chart)

