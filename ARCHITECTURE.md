# Wayfinder architecture

`index.html` is the app shell, `styles.css` defines its UI, and `app.js` owns route planning, saved trips, facility lookup, and journey state. Browser GPS feeds the progress tracker. TomTom geocoding and routing feed travel calculations, and trips and settings persist on-device. `sw.js` and `manifest.webmanifest` support installation and cached assets.

GitHub Pages serves `main` from the root. Keep developer tooling on a review branch unless publishing excludes it. Update cache and asset versions when runtime files change. Keep GPS, routing, saved-trip data, TomTom configuration, refresh limits and hosting under existing project rules.

See [TESTING.md](TESTING.md) for commands.
