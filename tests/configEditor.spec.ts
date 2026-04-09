import { test, expect } from '@grafana/plugin-e2e';
import { MyDataSourceOptions, MySecureJsonData } from '../src/types';

async function dismissWhatsNewModal(page: any) {
  const dialog = page.getByRole('dialog', { name: /what's new in grafana/i });
  if (await dialog.isVisible().catch(() => false)) {
    await dialog.getByRole('button', { name: /^close$/i }).click();
    await expect(dialog).toBeHidden({ timeout: 10000 });
  }
}

async function resetApiKey(page: any) {
  const resetButton = page.getByRole('button', { name: /^reset$/i });
  if (await resetButton.isVisible().catch(() => false)) {
    await resetButton.click();
  }
  await expect(page.getByLabel('API Key')).toBeEnabled();
}

async function saveConfiguration(page: any, uid: string) {
  await dismissWhatsNewModal(page);
  const saveResponse = page.waitForResponse(
    (response: any) => response.request().method() === 'PUT' && response.url().includes(`/api/datasources/uid/${uid}`)
  );
  await page.getByRole('button', { name: /save & test/i }).click();
  return saveResponse;
}

test('smoke: should render config editor', async ({ createDataSourceConfigPage, readProvisionedDataSource, page }) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await createDataSourceConfigPage({ type: ds.type });
  await dismissWhatsNewModal(page);
  await expect(page.getByLabel('Base URL')).toBeVisible();
  await expect(page.getByLabel('API Key')).toBeVisible();
});

test('"Save & test" should fail when configuration has invalid API key', async ({
  createDataSourceConfigPage,
  readProvisionedDataSource,
  request,
  page,
}) => {
  const ds = await readProvisionedDataSource<MyDataSourceOptions, MySecureJsonData>({ fileName: 'datasources.yml' });
  const configPage = await createDataSourceConfigPage({ type: ds.type });
  await page.getByLabel('Base URL').fill(ds.jsonData.baseUrl ?? 'https://api.gov.nominal.io');
  await page.keyboard.press('Tab');
  await resetApiKey(page);
  await page.getByLabel('API Key').fill('invalid-test-api-key');
  await page.keyboard.press('Tab');

  const saveResponse = await saveConfiguration(page, configPage.datasource.uid);
  expect(saveResponse.ok()).toBeTruthy();

  const healthResponse = await request.get(`/api/datasources/uid/${configPage.datasource.uid}/health`);
  expect(healthResponse.ok()).toBeFalsy();
  const healthBody = await healthResponse.json();
  expect(healthBody.status).toBe('ERROR');
});

test('"Save & test" should fail when configuration is missing API key', async ({
  createDataSourceConfigPage,
  readProvisionedDataSource,
  request,
  page,
}) => {
  const ds = await readProvisionedDataSource<MyDataSourceOptions, MySecureJsonData>({ fileName: 'datasources.yml' });
  const configPage = await createDataSourceConfigPage({ type: ds.type });
  await page.getByLabel('Base URL').fill(ds.jsonData.baseUrl ?? 'https://api.gov.nominal.io');
  await page.keyboard.press('Tab');
  await resetApiKey(page);

  const saveResponse = await saveConfiguration(page, configPage.datasource.uid);
  expect(saveResponse.ok()).toBeTruthy();

  const healthResponse = await request.get(`/api/datasources/uid/${configPage.datasource.uid}/health`);
  expect(healthResponse.ok()).toBeFalsy();
  const healthBody = await healthResponse.json();
  expect(healthBody.status).toBe('ERROR');
  expect(healthBody.message).toMatch(/API key.*required|Missing.*API key/i);
});
