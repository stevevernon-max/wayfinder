import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

test.beforeEach(async ({ page, baseURL }) => {
  await page.route("**/*", (route) => {
    const request = route.request();
    const url = new URL(request.url());
    if (
      url.origin !== new URL(baseURL).origin ||
      request.method() !== "GET" ||
      url.pathname.startsWith("/api/")
    ) {
      return route.abort();
    }
    return route.continue();
  });
  await page.goto("/");
});

test("primary workflow fits the viewport", async ({ page }) => {
  await expect(page.getByLabel("End", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Add stop", exact: true }).click();
  await expect(page.locator("#stops input").first()).toBeVisible();
  const fits = await page.evaluate(
    () =>
      document.documentElement.scrollWidth <=
      document.documentElement.clientWidth,
  );
  expect(fits).toBe(true);
});

test("@a11y reports WCAG findings", async ({ page }, testInfo) => {
  await expect(page.locator("main")).toBeVisible();
  const scan = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
    .analyze();
  const findings = scan.violations.map(({ id, impact, nodes }) => ({
    id,
    impact,
    targets: nodes.map(({ target }) => target),
  }));
  await testInfo.attach("axe-findings", {
    body: JSON.stringify(findings, null, 2),
    contentType: "application/json",
  });
  console.log(JSON.stringify({ project: testInfo.project.name, findings }));
});
