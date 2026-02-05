import { test, expect } from '@grafana/plugin-e2e';
import { MyDataSourceOptions, MySecureJsonData } from '../src/types';

test('smoke: should render config editor', async ({ createDataSourceConfigPage, readProvisionedDataSource, page }) => {
  const ds = await readProvisionedDataSource({ fileName: 'datasources.yml' });
  await createDataSourceConfigPage({ type: ds.type });
  await expect(page.getByLabel('Base URL')).toBeVisible();
  await expect(page.getByLabel('API Key')).toBeVisible();
});
test('"Save & test" should fail when configuration has invalid API key', async ({
  createDataSourceConfigPage,
  readProvisionedDataSource,
  page,
}) => {
  test.setTimeout(15000); // Set shorter timeout for this test
  
  const ds = await readProvisionedDataSource<MyDataSourceOptions, MySecureJsonData>({ fileName: 'datasources.yml' });
  const configPage = await createDataSourceConfigPage({ type: ds.type });
  await page.getByRole('textbox', { name: 'Base URL' }).fill(ds.jsonData.baseUrl ?? 'https://api.gov.nominal.io');
  await page.getByRole('textbox', { name: 'API Key' }).fill('invalid-test-api-key');
  
  // Since we're using an invalid API key, this should fail quickly with better error handling
  const result = configPage.saveAndTest();
  await expect(result).not.toBeOK();
  
  // Check that the error message is appropriate
  const alertElements = page.getByRole('alert');
  if (await alertElements.count() > 0) {
    const errorText = await alertElements.first().textContent();
    expect(errorText).toMatch(/API key|authentication|unauthorized|Failed to connect/i);
  }
});

test('"Save & test" should fail when configuration is missing API key', async ({
  createDataSourceConfigPage,
  readProvisionedDataSource,
  page,
}) => {
  test.setTimeout(15000); // Set shorter timeout for this test
  
  const ds = await readProvisionedDataSource<MyDataSourceOptions, MySecureJsonData>({ fileName: 'datasources.yml' });
  const configPage = await createDataSourceConfigPage({ type: ds.type });
  await page.getByRole('textbox', { name: 'Base URL' }).fill(ds.jsonData.baseUrl ?? 'https://api.gov.nominal.io');
  // Don't fill API Key to test failure case - should fail fast now
  const result = configPage.saveAndTest();
  await expect(result).not.toBeOK();
  
  // Check that the error message mentions missing API key
  const alertElements = page.getByRole('alert');
  if (await alertElements.count() > 0) {
    const errorText = await alertElements.first().textContent();
    expect(errorText).toMatch(/API key.*required|Missing.*API key/i);
  }
});
