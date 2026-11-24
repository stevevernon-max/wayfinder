# Development checks

- From the repository root, run `node --check app.js && node --check sw.js`. Preview has no build step. Use a separate fixture for routing and GPS.
- In `tools/quality` use Node 22.19+ (22.x) or 24+ and Python 3. Run `npm ci`, `npm run lint` and `npm run format:check`. These quality checks cover the tooling. Use targeted checks on modified app files.
- Run `npm run browsers` once, then `npm test -- --project "iPhone SE"` for a focused smoke check or `npm test` for shared changes. Profiles cover small and large iPhone WebKit and desktop Chromium. External providers and API writes are blocked.
- Run `npm run audit:a11y` and review the findings. Run `npm run audit:performance` with a local preview on port 8765 and Chrome available. Audit reports are diagnostic.
- Verify routing, manual stops, saved trips and the installed iPhone PWA. Use an approved real-device journey to validate GPS and compare it with simulation. Keep TomTom requests bounded.
- The Development quality workflow is manual. Enable the browser input for broader changes only. Use standard runners within the account's free allowance.
