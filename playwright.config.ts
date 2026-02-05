import type { PluginOptions } from '@grafana/plugin-e2e';
import { defineConfig, devices } from '@playwright/test';
import { dirname } from 'node:path';

const pluginE2eAuth = `${dirname(require.resolve('@grafana/plugin-e2e'))}/auth`;

/**
 * Read environment variables from file.
 * https://github.com/motdotla/dotenv
 */
require('dotenv').config();

/**
 * See https://playwright.dev/docs/test-configuration.
 */
export default defineConfig<PluginOptions>({
  testDir: './tests',
  /* Run tests in files in parallel */
  fullyParallel: true,
  /* Fail the build on CI if you accidentally left test.only in the source code. */
  forbidOnly: !!process.env.CI,
  /* Retry on CI only */
  retries: process.env.CI ? 2 : 0,
  /* Reporter to use. See https://playwright.dev/docs/test-reporters */
  reporter: 'html',
  /* Reduce global timeout from default 30s to 15s for faster feedback */
  timeout: 15000,
  /* Shared settings for all the projects below. See https://playwright.dev/docs/api/class-testoptions. */
  use: {
    /* Base URL to use in actions like `await page.goto('/')`. */
    baseURL: process.env.GRAFANA_URL || 'http://localhost:3000',

    /* Configure authentication credentials */
    grafanaAPICredentials: {
      user: process.env.GRAFANA_ADMIN_USER || 'admin',
      password: process.env.GRAFANA_ADMIN_PASSWORD || 'admin'
    },

    /* Collect trace when retrying the failed test. See https://playwright.dev/docs/trace-viewer */
    trace: 'on-first-retry',
    
    /* Set reasonable timeout for actions */
    actionTimeout: 10000,
  },

  /* Configure projects for major browsers */
  projects: [
    // 1. Login to Grafana and store the cookie on disk for use in other tests.
    {
      name: 'auth',
      testDir: pluginE2eAuth,
      testMatch: [/.*\.js/],
    },
    // 2. Run tests in Google Chrome. Every test will start authenticated as admin user.
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        storageState: 'playwright/.auth/admin.json',
      },
      dependencies: ['auth'],
    },
  ],
});
