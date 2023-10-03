# nodered-git-sync
A lightweight sidecar to reload nodered flows

Checklist
 - You get so nervous editing flows because you just know you're gonna break something
 - You have a development nodered instance where you make your changes. You use Nodered Projects to push to git.
 - You want to push your changes to your production instance by merging into a "prod" branch (for example).

 > NOTE: if there is existing code in the destination directory, it will be copied to `/backup/$(date +"%Y%m%d-%H%M%S")` inside the container.
 > If you don't mount this as a volume, it will get deleted if you ever recreate the container. Take that at your own risk.

 TODOs:
  - is git clone the best way to do things? perhaps we would want git init, git stash, etc. but that's more complicated
  - nodered has a few things standing in the way of automated bring up
    - git config user name and email - can fix this in two ways:
      - git config inside the nodered container
      - POST /settings/user {git: {user: {name, email}}}
    - setting the current project with decryption key
      - echo "{"projects": {$NODERED_PROJECTS_ACTIVE_PROJECT: {"credentialSecret": $NODERED_PROJECTS_CREDENTIAL_SECRET}}, "activeProject": $NODERED_PROJECTS_ACTIVE_PROJECT}" > .config.projects.json
{
    "projects": {
        "asdf": {
            "credentialSecret": "asdf"
        }
    },
    "activeProject": "asdf"
}

### Public nodered project repo
put here: [nodered values.yaml](https://github.com/SchwarzIT/node-red-chart/blob/main/charts/node-red/values.yaml)
```yaml
extraSidecars:
- name: git-sync
  image: ghcr.io/beasteers/nodered-git-sync:main
  env:
  # git options
  - name: GITSYNC_REPO
    value: https://github.com/me/my-repo
  - name: GITSYNC_REF
    value: prod
  - name: GITSYNC_PERIOD
    value: "5m"

  # nodered API credentials (to reload)
  - name: NODERED_USERNAME
    value: admin
  - name: NODERED_PASSWORD
    valueFrom:
      secretKeyRef:
      key: ADMIN_PASSWORD
      name: nodered-auth

  volumeMounts: 
  # mount nodered's directory
  - name: data
    mountPath: /data
  securityContext:
    runAsGroup: 65533  # git-sync user id
```

Put your credentials in a secret so you can access them from the container.
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: nodered-auth
  namespace: cluster-config
data:
  ADMIN_USERNAME: admin
  ADMIN_PASSWORD: boop
  POSTGRES_USER: admin
  POSTGRES_PASS: boop
```

### If your repo is private
```yaml
extraSidecars:
- name: git-sync
  image: ghcr.io/beasteers/nodered-git-sync:main
  env:
  # git options
  - name: GITSYNC_REPO
    value: git@github.com:me/my-repo
  - name: GITSYNC_REF
    value: main
  - name: GITSYNC_SSH
    value: "true"
  - name: GITSYNC_PERIOD
    value: "5m"

  # nodered API credentials (to reload)
  - name: NODERED_USERNAME
    value: admin
  - name: NODERED_PASSWORD
    valueFrom:
      secretKeyRef:
      key: ADMIN_PASSWORD
      name: nodered-auth

  volumeMounts: 
  # git ssh credentials (to pull)
  - name: git-nodered-ssh-key
    mountPath: /etc/git-secret
  # mount nodered's directory
  - name: data
    mountPath: /data
  securityContext:
    runAsGroup: 65533  # git-sync user id

extraVolumes:
# git ssh credentials
- name: git-nodered-ssh-key
  secret:
      secretName: git-nodered-ssh-key   
```

Put your credentials in a secret so you can access them from the container.
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: nodered-auth
  namespace: cluster-config
data:
  ADMIN_USERNAME: admin
  ADMIN_PASSWORD: boop
  POSTGRES_USER: admin
  POSTGRES_PASS: boop
---
apiVersion: v1
kind: Secret
metadata:
  name: git-nodered-ssh-key
  namespace: cluster-config
type: Opaque
stringData:
  ssh: |
    -----BEGIN OPENSSH PRIVATE KEY-----
    ...
    -----END OPENSSH PRIVATE KEY-----
  known_hosts: |
    github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=
    github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl
    github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=
```

### Why does this exist? Aren't there other ways?

The helm chart's built-in sidecar
 - reads flows and files from configmaps
   - this is okay - you can use this with flux, however it would require generating a kustomization file...
 - package installs are handled in a non-standard way
   - they need to be added to `extra-node-modules.json` so packages installed via the UI wouldn't be tracked
 - hard-coded to /data so it doesn't support the projects feature.


git-sync - direct way
```yaml
GITSYNC_ROOT=/git/projects

volumeMounts:
  - name: data
    mountPath: /git
```
 - this is almost there! but not quite
 - it clones in nodered to `/data/projects/repo-name`
 - the problem is that it uses symlinks (for atomic updates) but nodered has a problem with that
    - the project name is not the name of the symlink, it's the name of the worktree (which will change and break the link to the project - and is not to mention hard to reason about)
    - even if you select the project it complains about the project being empty (no project.json)
    - if you copy the working tree directly into `/data/projects` it works fine

git-sync - copy files over
```
# script.sh
rsync /git/src /data/projects/repo-name
```
 - so you may think - okay well why can't we just rsync the files after doing a git pull?
   - firstly, we'd need to add rsync to the container, but it's based off scratch so there's no package installer in the container
   - you'd want to make sure that rsync copies .git so that you have the git working tree
   - it's also important so you can git push if you have changes.
 - I don't think any of these are insurmountable so if you can figure it out, nice!

 git-sync - no projects just mount
 ```yaml
GITSYNC_ROOT=/git/src

persistance: 
  subPath: /src/repo-name
 ```
  - okay so maybe we should just disable projects feature and mount directly to /data
  - could be trouble cloning to the root of a volume emount
  - instead mount to a subdirectory

git clone directly
- won't have git pull updates


The goals of this solution:
 - git clone directly to the folder
 - git pull periodically
 - don't try to pull if there are changes


Resources:
 - 
 - https://medium.com/@knolleary/creating-a-node-red-deployment-pipeline-to-ibm-cloud-9a8e9d5113de