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

test('smoke: should render config editor', async ({ createDataSourceConfigPage, readProvisionedDataSource, page }) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await createDataSourceConfigPage({ type: ds.type });
  await dismissWhatsNewModal(page);
  await expect(page.getByLabel('Base URL')).toBeVisible();
  await expect(page.getByLabel('API Key')).toBeVisible();
});
test('should allow replacing a configured API key', async ({
  createDataSourceConfigPage,
  readProvisionedDataSource,
  page,
}) => {
  const ds = await readProvisionedDataSource<MyDataSourceOptions, MySecureJsonData>({ fileName: 'datasources.yml' });
  await createDataSourceConfigPage({ type: ds.type });
  await page.getByLabel('Base URL').fill(ds.jsonData.baseUrl ?? 'https://api.gov.nominal.io');
  await resetApiKey(page);
  await page.getByLabel('API Key').fill('invalid-test-api-key');
  await expect(page.getByLabel('API Key')).toHaveValue('invalid-test-api-key');
});

test('should make the API key editable after reset', async ({
  createDataSourceConfigPage,
  readProvisionedDataSource,
  page,
}) => {
  const ds = await readProvisionedDataSource<MyDataSourceOptions, MySecureJsonData>({ fileName: 'datasources.yml' });
  await createDataSourceConfigPage({ type: ds.type });
  await page.getByLabel('Base URL').fill(ds.jsonData.baseUrl ?? 'https://api.gov.nominal.io');
  await resetApiKey(page);
  await expect(page.getByLabel('API Key')).toHaveValue('');
});
