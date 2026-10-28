// Jest setup provided by Grafana scaffolding
import './.config/jest-setup';
import { EventBusSrv } from '@grafana/data';
import { setAppEvents } from '@grafana/runtime';

setAppEvents(new EventBusSrv());

HTMLCanvasElement.prototype.getContext = () => ({
  measureText: (text) => ({ width: String(text).length * 8 }),
});
