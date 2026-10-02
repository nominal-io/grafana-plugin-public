import { AppEvents } from '@grafana/data';
import { getAppEvents } from '@grafana/runtime';

export const notifyError = (title: string, message: string) => {
  getAppEvents().publish({
    type: AppEvents.alertError.name,
    payload: [title, message],
  });
};
