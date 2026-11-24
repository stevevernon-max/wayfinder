import { defineConfig, devices } from "@playwright/test";

const baseURL = "http://127.0.0.1:8765";

export default defineConfig({
  testDir: "./tests",
  reporter: "line",
  use: { baseURL, serviceWorkers: "block", screenshot: "only-on-failure" },
  webServer: {
    command:
      "python3 -m http.server 8765 --bind 127.0.0.1 --directory ../../.",
    url: baseURL,
  },
  projects: [
    { name: "iPhone SE", use: devices["iPhone SE"] },
    { name: "iPhone 15 Pro Max", use: devices["iPhone 15 Pro Max"] },
    { name: "Desktop Chromium", use: devices["Desktop Chrome"] },
  ],
});
