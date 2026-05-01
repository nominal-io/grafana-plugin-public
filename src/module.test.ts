import { plugin } from './module';

describe('plugin registration', () => {
  it('registers query editor help', () => {
    expect(plugin.components.QueryEditorHelp).toBeDefined();
  });
});
