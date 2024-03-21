<template>
  <div>
    <div class="mb-1">
      <h1 class="text-xl">MLB Standard Batting</h1>
    </div>
    <div class="w-full mb-2">
      <table class="border text-center rounded-md w-full p-1">
        <thead class="bg-neutral-100">
          <tr>
            <th class="py-2 px-1">Season</th>
            <th class="py-2 px-1">Team</th>
            <th class="py-2 px-1">League</th>
            <th class="py-2 px-1">G</th>
            <th class="py-2 px-1">PA</th>
            <th class="py-2 px-1">AB</th>
            <th class="py-2 px-1">R</th>
            <th class="py-2 px-1">H</th>
            <th class="py-2 px-1">2B</th>
            <th class="py-2 px-1">3B</th>
            <th class="py-2 px-1">HR</th>
            <th class="py-2 px-1">RBI</th>
            <th class="py-2 px-1">BB</th>
            <th class="py-2 px-1">SO</th>
            <th class="py-2 px-1">SB</th>
            <th class="py-2 px-1">CS</th>
            <th class="py-2 px-1">HBP</th>
            <th class="py-2 px-1">BA</th>
            <th class="py-2 px-1">OBP</th>
            <th class="py-2 px-1">SLG</th>
            <th class="py-2 px-1">OPS</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="d of stdBattingData">
            <td class="py-2 px-1">{{ d.game_season }}</td>
            <td class="py-2 px-1">{{ d.team_name }}</td>
            <td class="py-2 px-1">{{ d.league_name }}</td>
            <td class="py-2 px-1">{{ d.G }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'PA')">{{ d.PA }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'AB')">{{ d.AB }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'R')">{{ d.R }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'H')">{{ d.H }}</td>
            <td class="py-2 px-1" :class="getShade(d, '2B')">{{ d["2B"] }}</td>
            <td class="py-2 px-1" :class="getShade(d, '3B')">{{ d["3B"] }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'HR')">{{ d.HR }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'RBI')">{{ d.RBI }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'BB')">{{ d.BB }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'SO')">{{ d.SO }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'SB')">{{ d.SB }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'CS')">{{ d.CS }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'HBP')">{{ d.HBP }}</td>
            <td class="py-2 px-1" :class="getShade(d, 'BA')">
              {{ d.BA.toFixed(3) }}
            </td>
            <td class="py-2 px-1" :class="getShade(d, 'OBP')">
              {{ d.OBP.toFixed(3) }}
            </td>
            <td class="py-2 px-1" :class="getShade(d, 'SLG')">
              {{ d.SLG.toFixed(3) }}
            </td>
            <td class="py-2 px-1" :class="getShade(d, 'OPS')">
              {{ d.OPS.toFixed(3) }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- percentile legend -->
    <div class="text-xs">
      <div class="flex flex-row-reverse justify-center mb-2">
        <template v-for="(cls, perc) in topPercShades">
          <div class="flex flex-row items-center" :key="perc">
            <div class="w-4 h-4 rounded-full mr-1" :class="cls"></div>
            <div class="mr-2">{{ toPercLabel(perc) }}</div>
          </div>
        </template>
      </div>
    </div>
  </div>
</template>

<script>
const TOP_PERC_SHADES = {
  0.95: "bg-green-800",
  0.9: "bg-green-600",
  0.85: "bg-green-400",
  0.8: "bg-green-200",
};

// not currently used
const BOTTOM_PERC_SHADES = {
  0.05: "bg-orange-500",
  0.1: "bg-orange-300",
  0.15: "bg-orange-300",
  0.2: "bg-orange-100",
};

export default {
  name: "StandardBatting",

  props: {
    stdBattingData: {
      type: Array,
      required: true,
    },
  },

  methods: {
    getShade(obj, key) {
      let perc = obj[key + "_perc"];
      if (perc === undefined) {
        return "";
      }

      for (let [k, v] of Object.entries(TOP_PERC_SHADES)) {
        if (perc >= k) {
          return v;
        }
      }
    },

    toPercLabel(perc, top = true) {
      if (top) {
        return `Top ${((1 - perc) * 100).toFixed(0)}%`;
      } else {
        return `Bottom ${(perc * 100).toFixed(0)}%`;
      }
    },
  },

  computed: {
    topPercShades() {
      return TOP_PERC_SHADES;
    },

    bottomPercShades() {
      return BOTTOM_PERC_SHADES;
    },
  },
};
</script>
