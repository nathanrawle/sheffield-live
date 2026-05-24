# Deployment

This project can run on a single small Ubuntu VPS:

- `sheaflive-web` runs as a long-lived `systemd` service
- `sheaflive-ingest` runs as a `systemd` one-shot service
- `sheaflive-ingest.timer` schedules ingest every 6 hours
- Caddy terminates public HTTPS and reverse proxies to `127.0.0.1:8080`
- SQLite and copied media live under `/var/lib/sheaflive`

The bootstrap script targets a fresh Ubuntu/Debian-style VPS. It is intentionally a plain Bash script so each server change remains inspectable.

## What The Script Does

`scripts/bootstrap-vps.sh`:

- installs base packages, SQLite CLI, Git, and Go `1.25.10`
- creates the locked-down `sheaflive` service user
- creates `/opt/sheaflive` and `/var/lib/sheaflive`
- clones the repository and builds the three Go entrypoints
- installs the web and ingest binaries plus repo-backed `config/`
- writes `/etc/sheaflive.env`
- writes and enables `systemd` units for web and scheduled ingest
- installs and configures Caddy unless skipped
- configures UFW unless skipped
- optionally creates/updates a human sudo user and hardens SSH

It does not configure DNS records. Point the domain at the VPS before expecting public HTTPS to work:

```text
A      @      <vps-ipv4>
CNAME  www    <domain>
```

Remove any stale root `AAAA` record unless IPv6 has also been deliberately configured on the VPS.

## Fresh VPS Example

Copy the script to the server:

```bash
scp scripts/bootstrap-vps.sh root@<vps-ip>:/tmp/bootstrap-vps.sh
```

Then SSH to the server and run it:

```bash
ssh root@<vps-ip>
bash /tmp/bootstrap-vps.sh \
  --domain sheaflive.com \
  --repo-url https://github.com/OWNER/REPO.git \
  --admin-user nathan \
  --ssh-public-key 'ssh-ed25519 AAAA... comment' \
  --harden-ssh
```

If the repo is private, use a clone URL and server credentials that Git can access. For example, install a deploy key first and use the SSH clone URL.

If the public key already exists as a file on the VPS, use `--ssh-public-key-file /path/to/key.pub` instead of pasting the key line.

If you already have an admin bcrypt hash:

```bash
bash /tmp/bootstrap-vps.sh \
  --domain sheaflive.com \
  --repo-url https://github.com/OWNER/REPO.git \
  --admin-password-hash '$2a$10$...'
```

If `--admin-password-hash` is omitted, the script prompts for the admin passphrase and hashes it with `cmd/admin-password-hash`.

## SSH Hardening

SSH hardening is opt-in because it can lock you out if the key or user is wrong.

With `--harden-ssh`, the script:

- creates or updates `--admin-user`
- adds the user to the `sudo` group
- installs the supplied public key into `authorized_keys`
- writes `/etc/ssh/sshd_config.d/01-sheaflive-hardening.conf`
- disables root login and password login
- requires public-key authentication

Before closing the original root session, open a new terminal and verify:

```bash
ssh nathan@<vps-ip>
sudo true
ssh -o PreferredAuthentications=password -o PubkeyAuthentication=no nathan@<vps-ip>
ssh -i ~/.ssh/sheaflive_prod_ionos root@<vps-ip>
```

The first two commands should work. The password-only and root logins should fail.

## After Bootstrap

Check local app health on the VPS:

```bash
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
```

Check services:

```bash
systemctl status sheaflive-web --no-pager
systemctl list-timers sheaflive-ingest.timer --no-pager
journalctl -u sheaflive-web -n 80 --no-pager
journalctl -u sheaflive-ingest -n 80 --no-pager
```

After DNS has propagated and Caddy has issued certificates:

```bash
curl -I https://sheaflive.com/healthz
curl -I https://www.sheaflive.com/healthz
```

The `www` host should redirect to the bare domain.

## Existing Data

The bootstrap script leaves `/var/lib/sheaflive` in place. Re-running it updates binaries, config files, service files, Caddy, and `/etc/sheaflive.env`, but it does not delete the SQLite database or media directory.

Back up these paths before any real migration or rebuild:

```text
/var/lib/sheaflive/sheaflive.db
/var/lib/sheaflive/media
```
