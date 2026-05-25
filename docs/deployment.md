# Deployment

This project can run on a single small Ubuntu VPS:

- `sheaflive-web` runs as a long-lived `systemd` service
- `sheaflive-ingest` runs as a `systemd` one-shot service
- `sheaflive-ingest.timer` schedules ingest every 6 hours
- Caddy terminates public HTTPS and reverse proxies to `127.0.0.1:8080`
- SQLite and copied media live under `/var/lib/sheaflive`

The bootstrap script targets a fresh Ubuntu/Debian-style VPS. It is intentionally a plain Bash script so each server change remains inspectable.
Repeated application updates should use `scripts/deploy-vps.sh`; bootstrap delegates binary/config installation to that script after the machine-level setup is in place.

## What The Script Does

`scripts/bootstrap-vps.sh`:

- installs base packages, SQLite CLI, Git, and Go `1.25.10`
- creates the locked-down `sheaflive` service user
- creates `/opt/sheaflive` and `/var/lib/sheaflive`
- clones the repository
- writes `/etc/sheaflive.env`
- writes and enables `systemd` units for web and scheduled ingest
- runs `scripts/deploy-vps.sh` to build/install binaries, copy repo-backed `config/`, and restart the web service
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
- requires that user to have a usable password for `sudo`
- adds the user to the `sudo` group
- installs the supplied public key into `authorized_keys`
- writes `/etc/ssh/sshd_config.d/01-sheaflive-hardening.conf`
- disables root login and password login
- requires public-key authentication

If the admin user does not already exist, bootstrap creates it interactively and asks for a password. If the user exists but has no usable password, bootstrap asks you to set one before SSH hardening is applied. In non-interactive runs, create the user and password first.

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

## Deploy Updates

After bootstrap, use `scripts/deploy-vps.sh` for normal application updates. It builds the web and ingest binaries from a repository ref, installs them into `/opt/sheaflive`, copies `config/`, restarts `sheaflive-web.service`, and checks local health/readiness.

From a copied script:

```bash
sudo bash /tmp/deploy-vps.sh \
  --repo-url https://github.com/OWNER/REPO.git \
  --repo-ref main
```

From a checkout on the VPS:

```bash
sudo bash scripts/deploy-vps.sh --source-dir "$PWD"
```

The deploy script does not rewrite `/etc/sheaflive.env`, systemd unit files, Caddy, SSH, or firewall settings.

## Add Another Domain

Use this when adding another domain name that should serve the same app.

First point DNS for the new domain at the VPS:

```text
A      @      <vps-ipv4>
CNAME  www    <domain>
```

Remove any default root `AAAA` record unless IPv6 has also been configured on the VPS, in the provider firewall, in UFW, and in Caddy. Otherwise some visitors may resolve the new domain over IPv6 and reach the wrong host.

Check the authoritative DNS result before changing Caddy:

```bash
dig +short example.com A
dig +short example.com AAAA
dig +short www.example.com CNAME
dig +short www.example.com A
```

Expected:

- root `A` returns the VPS IPv4 address
- root `AAAA` returns no output unless IPv6 is deliberately configured
- `www` is a CNAME to the root domain
- `www` resolves to the VPS IPv4 address

Add the new domain to `/etc/caddy/Caddyfile`:

```caddyfile
www.example.com {
    redir https://example.com{uri} permanent
}

example.com {
    reverse_proxy 127.0.0.1:8080
}
```

Validate and reload Caddy:

```bash
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
```

Caddy should automatically request and store certificates for the new hostnames. Watch the logs:

```bash
sudo journalctl -u caddy -n 120 --no-pager | grep -E 'certificate|obtaining|obtained|renew|tls'
```

Verify the public routes:

```bash
curl -I https://example.com/healthz
curl -I https://www.example.com/healthz
```

The bare domain should return `200`, and `www` should return a permanent redirect to the bare domain.

Inspect the served certificate:

```bash
echo | openssl s_client -connect example.com:443 -servername example.com 2>/dev/null \
  | openssl x509 -noout -subject -issuer -dates
```

The issuer should be Let's Encrypt or another Caddy-configured ACME issuer, and the `notAfter` date should be in the future.

## Existing Data

The bootstrap script leaves `/var/lib/sheaflive` in place. Re-running it updates binaries, config files, service files, Caddy, and `/etc/sheaflive.env`, but it does not delete the SQLite database or media directory.

Back up these paths before any real migration or rebuild:

```text
/var/lib/sheaflive/sheaflive.db
/var/lib/sheaflive/media
```
