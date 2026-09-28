# Todo

## Retire the static `COMPUTE_SERVER_TOKEN`

`compute_server_base/auth.py` accepts two credentials: a dashboard-issued JWT
verified by local HMAC plus an `aud: tool:<name>` check, and a single static
`COMPUTE_SERVER_TOKEN` shared by every caller. The static one is a holdover. It
stays only because MatFlow still uses it, and MatFlow is paused, so nobody is
watching it to catch a break.

MatFlow's stored token is already dead — it stopped working when
materials-framework was reprovisioned from the dashboard — so its relax node cannot
authenticate today either way.

What has to happen, in order:

1. MatFlow is unpaused. Nothing below is urgent until then.
2. Issue it a subject of its own: `POST /api/tools/materials-framework/clients` on
   the dashboard.
3. Give it a route. Like the dashboard, a container cannot reach `127.0.0.1:8300`,
   so MatFlow must join `spark-network` and address the tool by container name.
4. Repoint `MaterialsFrameworkRelaxNode` at `compute_interface` while it is being
   touched anyway — it still calls the deprecated `/mlips`, which is the other
   reason that shim is still served.
5. Delete the static-token branch from `auth.py`. Every caller is then on signed
   tokens, and rotation becomes a real revocation mechanism rather than a partial
   one.

The dashboard-side step is tracked in `spark-dashboard/docs/todo.md`; the deletion
is this repository's.
