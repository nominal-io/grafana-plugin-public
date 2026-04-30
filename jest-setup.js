// Jest setup provided by Grafana scaffolding
import './.config/jest-setup';

HTMLCanvasElement.prototype.getContext = () => ({
  measureText: (text) => ({ width: String(text).length * 8 }),
});
