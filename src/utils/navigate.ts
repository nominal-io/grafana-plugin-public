// Leaves the page for url; a module of its own so tests can stub full-page navigation.
export function navigateTo(url: string) {
  window.location.assign(url);
}
