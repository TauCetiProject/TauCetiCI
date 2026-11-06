/** Approved public aliases; permanent IDs verified against enrollment inventory.
 * New dedicated hosts are also supplied by the signed/scoped host registry. */
export const RUNNER_HOSTS = [
  {
    machine_id: "4fab2fed9a5145f894d066c7bb28ace4",
    alias: "eastanglia",
    label: "Chris / eastanglia",
    aliases: ["chris-server", "chris"],
  },
  {
    machine_id: "11e2756fc11042d798f759410573a935",
    alias: "leanfro",
    label: "Tauchonki / leanfro",
    aliases: ["tauchonki"],
  },
  {
    machine_id: "a26dbb02840844f1a72450e8cc29ea28",
    alias: "yael",
    label: "Yaël",
    aliases: ["yael-server", "yael"],
  },
  {
    machine_id: "0fb9966f7c404b35b7c55ae6a6283f87",
    alias: "brett",
    label: "Brett",
    aliases: ["brett-server", "brett"],
  },
];
export const PUBLIC_POOLS = [
  {
    id: "github",
    label: "GitHub hosted",
    resources: "unknown",
    inventory: "installation_jobs",
  },
  {
    id: "namespace",
    label: "Namespace",
    resources: "requested_profile_only",
    inventory: "installation_jobs",
  },
  ...RUNNER_HOSTS.map((h) => ({
    id: h.alias,
    label: h.label,
    machine_id: h.machine_id,
    resources: "host_observer",
    inventory: "configured",
  })),
  {
    id: "self-hosted",
    label: "Other self-hosted",
    resources: "unknown",
    inventory: "installation_jobs",
  },
];
