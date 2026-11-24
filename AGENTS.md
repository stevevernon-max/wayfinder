# Wayfinder project instructions

## Project
- Active repository: stevevernon-max/wayfinder. Hosting: GitHub Pages, main branch.
- Static HTML, CSS and JavaScript. TomTom requests run in the browser.
- Preserve GPS tracking, routing, facility searches, saved trips, import/export and home-screen support.

## Minimise usage and costs
- Inspect only relevant files and read existing code before editing.
- Combine related requested changes into one tested commit and deployment.
- Do not deploy each small intermediate edit.
- Use focused checks appropriate to the change. Repeat only after new changes or failures.
- Keep tool output and progress reports concise. Avoid repeated polling of unchanged state.
- Reuse current dependencies and implementation. Avoid unnecessary packages and services.
- Do not create agents or paid agent runs unless the user explicitly requests them.
- Do not enable paid hosting, paid plans, automatic top-ups, new metered services, or billing changes without explicit approval.
- Do not use Netlify or Cloudflare deployments for this app. Work in this GitHub Pages repository.
- Instructions guide behaviour; they do not enforce account budgets or guarantee zero usage.

## API usage
- Reuse valid place coordinates and route results.
- Debounce autocomplete and cancel obsolete requests.
- GPS position updates must not automatically trigger routing or facility searches on every fix.
- Avoid continuous background API polling. Refresh routing when needed or requested.
- Bound facility searches and retain deduplication.
- Test external APIs with a small number of representative requests.

## Security and data
- tomtom-config.js contains an intentionally public browser key restricted to the approved GitHub Pages domain. Do not print it in logs or chat.
- Never commit private server credentials or unrelated account secrets.
- Keep device trips and settings intact during changes.
- Do not change repository visibility, permissions, or account settings without the necessary approval.

## Verification and publishing
- Check JavaScript syntax and affected behaviour before committing.
- Validate small-screen layout when changing UI.
- After publishing, verify the specific requested changes.
- Distinguish simulation results from real-device GPS testing.
- Report unresolved failures honestly and avoid speculative repeated fixes.
- Update asset/cache versions when client files change.

## Credit-efficient workflow

- Match reasoning to the task. Use low for copy, colours and spacing, medium for routine work, high for complex bugs, GPS, authentication and live integrations, and extra high when the model supports it for major architecture or exceptional debugging.
- Start with `rg --files` and targeted `rg -n` searches. Use semantic navigation when a symbol or its callers are hard to locate. Read only relevant sections and reuse information already retrieved.
- Make targeted edits using existing components, functions, utilities and design tokens. Keep unrelated code and dependencies out of the diff. Preserve existing behaviour and integrations.
- Group related changes in one cycle. Use deterministic linting, formatting and tests. Run targeted tests first and broader regressions when shared behaviour, authentication, integrations or architecture warrant them.
- Use MCP selectively. Capture screenshots and repeat visual audits only when needed. Keep tool output, progress and final summaries concise.
- Review the diff, test the completed change and commit it to GitHub. Open [ARCHITECTURE.md](ARCHITECTURE.md), [DESIGN.md](DESIGN.md) or [TESTING.md](TESTING.md) when relevant.
