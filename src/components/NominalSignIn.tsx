import React, { useCallback, useEffect, useState } from 'react';
import { Alert, Button, Stack } from '@grafana/ui';
import type { DataSource } from '../datasource';

interface Props {
  datasource: DataSource;
  onSignedIn: () => void;
}

const POLL_INTERVAL_MS = 1000;

// Prompts for a per-user Nominal sign-in when the data source uses OAuth.
export function NominalSignIn({ datasource, onSignedIn }: Props) {
  const [signedIn, setSignedIn] = useState<boolean | undefined>(undefined);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    if (!datasource.usesOAuth) {
      return;
    }
    datasource.getOAuthStatus().then(setSignedIn, () => setSignedIn(false));
  }, [datasource]);

  const signIn = useCallback(async () => {
    setError(undefined);
    const popup = window.open('', 'nominal-sign-in', 'width=520,height=680');
    if (!popup) {
      setError('Allow popups for this site to sign in to Nominal.');
      return;
    }
    setPending(true);
    try {
      popup.location.href = await datasource.startOAuthSignIn();
    } catch (e) {
      popup.close();
      setPending(false);
      setError('Could not start the Nominal sign-in.');
      return;
    }
    const timer = window.setInterval(async () => {
      const closed = popup.closed;
      const nowSignedIn = await datasource.getOAuthStatus().catch(() => false);
      if (!nowSignedIn && !closed) {
        return;
      }
      window.clearInterval(timer);
      setPending(false);
      setSignedIn(nowSignedIn);
      if (nowSignedIn) {
        popup.close();
        onSignedIn();
      }
    }, POLL_INTERVAL_MS);
  }, [datasource, onSignedIn]);

  const signOut = useCallback(async () => {
    await datasource.signOutOAuth();
    setSignedIn(false);
  }, [datasource]);

  if (!datasource.usesOAuth || signedIn === undefined) {
    return null;
  }
  if (signedIn) {
    return (
      <Stack direction="row" justifyContent="flex-end">
        <Button size="sm" variant="secondary" fill="text" onClick={signOut}>
          Sign out of Nominal
        </Button>
      </Stack>
    );
  }
  return (
    <Alert title="Sign in to Nominal" severity="info">
      <Stack direction="column" gap={1}>
        <div>This data source queries Nominal as you. Sign in to see your data.</div>
        {error && <div>{error}</div>}
        <div>
          <Button onClick={signIn} disabled={pending} icon={pending ? 'spinner' : undefined}>
            {pending ? 'Waiting for sign-in…' : 'Sign in to Nominal'}
          </Button>
        </div>
      </Stack>
    </Alert>
  );
}
