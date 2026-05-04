# Trevas documentation

## Prerequisites

- [Node.js](https://nodejs.org/) **18 or newer** (see `engines` in `package.json`). The `.nvmrc` in this folder and GitHub Actions use **Node 24** so local and CI environments stay aligned.
- With [nvm](https://github.com/nvm-sh/nvm), from `docs/` run `nvm install` (if needed) then `nvm use` to pick up `.nvmrc`.

## Local Development

```console
yarn start
```

This command starts a local development server and open up a browser window. Most changes are reflected live without having to restart the server.

## Build

```console
yarn build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

## Deployment

```console
GIT_USER=<Your GitHub username> USE_SSH=true yarn deploy
```

If you are using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.

## Testing the local site

```console
yarn serve
```
