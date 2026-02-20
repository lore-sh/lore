## Purpose
- Keep current UI visuals stable while making future redesign migration easy.
- Follow Tailwind CSS v4.2+ CSS-first architecture.

## Style Architecture
- Entry: `src/client/styles.css`
- Tokens/Themes: `src/client/styles/theme.css`
- Base: `src/client/styles/base.css`
- Reusable UI recipes: `src/client/styles/components.css`

## Tailwind Rules
- Use `@import "tailwindcss" source("../")` in the entry CSS.
- Define semantic tokens with `@theme`.
- Define dark mode selector with `@custom-variant dark` and `data-theme`.
- Keep repeated patterns in `@layer components` as `ui-*` classes.

## Naming
- Raw theme variables: `--studio-*`
- Semantic exposed tokens: `--color-*` in `@theme`
- Reusable component classes: `ui-*`

## Authoring Policy
- Do not hardcode palette colors in TSX.
- Prefer semantic utilities (`text-fg`, `bg-bg-elevated`) or `ui-*` classes.
- Keep one-off layout utilities in TSX (spacing, grid, flex).
- When a class pattern appears in multiple screens, promote it to `components.css`.

## Theme Strategy
- Default theme is `data-theme="light"`.
- Dark mode is enabled by token overrides under `[data-theme="dark"]`.
- Future theme switcher should only toggle `document.documentElement.dataset.theme`.
