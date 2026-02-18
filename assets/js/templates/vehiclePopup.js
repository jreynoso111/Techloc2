export const vehiclePopupTemplate = ({
  modelYear = 'Vehicle',
  vin = 'N/A',
  status = 'ACTIVE',
  customer = 'Customer pending',
  lastLocation = 'No location provided',
  locationNote = '',
  accuracyDot = '',
  gpsFix = 'Unknown',
  dealCompletion = '—'
} = {}) => `
  <div class="w-[240px] bg-slate-950/90 text-white">
    <div class="p-3 space-y-2">
      <div class="flex items-start justify-between gap-2">
        <div class="space-y-1">
          <p class="text-[10px] font-black uppercase tracking-[0.15em] text-amber-400">Vehicle</p>
          <h3 class="text-base font-extrabold leading-tight text-white">${modelYear}</h3>
          <p class="text-[11px] text-slate-300">VIN ${vin}</p>
        </div>
        <span class="inline-flex items-center gap-1 rounded-full bg-amber-500/15 px-2 py-1 text-[10px] font-semibold text-amber-200 border border-amber-400/40">${status}</span>
      </div>

      <div class="text-[12px] text-slate-200 space-y-1">
        <p class="font-semibold">${customer}</p>
        <p class="flex items-center gap-2">
          <span class="inline-flex h-2 w-2 rounded-full ${accuracyDot}"></span>
          <span>${lastLocation}</span>
        </p>
        ${locationNote ? `<p class="text-[11px] text-amber-200 font-semibold">${locationNote}</p>` : ''}
      </div>

      <div class="grid grid-cols-2 gap-2 text-[11px]">
        <div class="rounded-lg border border-slate-800 bg-slate-900/80 px-2 py-2">
          <p class="text-[9px] uppercase text-slate-500 font-bold">GPS</p>
          <p class="font-semibold text-slate-50">${gpsFix}</p>
        </div>
        <div class="rounded-lg border border-slate-800 bg-slate-900/80 px-2 py-2">
          <p class="text-[9px] uppercase text-slate-500 font-bold">Deal %</p>
          <p class="font-semibold text-slate-50">${dealCompletion}</p>
        </div>
      </div>

      <div class="flex items-center justify-end">
        <button type="button" data-view-more-popup class="inline-flex items-center gap-1.5 rounded-lg border border-amber-400/50 bg-amber-500/15 px-3 py-1 text-[10px] font-bold text-amber-100 hover:bg-amber-500/25 transition-colors">
          More details
        </button>
      </div>
    </div>
  </div>
`;
