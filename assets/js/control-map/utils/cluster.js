export const createPartnerClusterIcon = (cluster, color = '#334155') => {
  const count = cluster.getChildCount();
  return L.divIcon({
    html: `<div class="relative text-xs font-semibold rounded-full flex items-center justify-center w-8 h-8 border-2 shadow-md bg-slate-900/90" style="color:${color}; border-color:${color}">${count}</div>`,
    className: 'partner-cluster-icon',
    iconSize: [32, 32],
    iconAnchor: [16, 16]
  });
};

export const createPartnerClusterGroup = (color = '#334155') => L.markerClusterGroup({
  maxClusterRadius: 45,
  disableClusteringAtZoom: 17,
  spiderfyOnMaxZoom: true,
  showCoverageOnHover: false,
  iconCreateFunction: (cluster) => createPartnerClusterIcon(cluster, color)
});
