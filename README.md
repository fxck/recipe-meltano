# Zerops x Meltano

Import:

```yaml
project:
  name: meltano-recipe

services:
  - hostname: db
    type: postgresql@16
    mode: NON_HA
    priority: 10

  - hostname: meltano
    type: python@3.11
    buildFromGit: https://github.com/fxck/recipe-meltano
    enableSubdomainAccess: true
    envSecrets:
      WORKERS: 1
```

...
