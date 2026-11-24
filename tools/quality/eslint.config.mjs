import js from "@eslint/js";
import globals from "globals";

export default [
  { ignores: ["playwright-report/**", "test-results/**", "reports/**"] },
  js.configs.recommended,
  {
    files: ["**/*.mjs"],
    languageOptions: { globals: globals.node },
  },
  {
    files: ["tests/**/*.mjs"],
    languageOptions: { globals: { document: "readonly" } },
  },
];
