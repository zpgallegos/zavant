<template>
  <div class="border border-gray-200 rounded-md p-4">
    <div class="border-b border-black mb-2">
      <h1 class="text-2xl">{{ info.player_name }}</h1>
    </div>
    <div>
      <span>
        <span> {{ info.primaryPosition_abbreviation }} </span
        ><span class="text-blue-500">&nbsp;|&nbsp;</span>
        <span>B/T: {{ info.batSide_code }}/{{ info.pitchHand_code }}</span>
        <span class="text-blue-500">&nbsp;|&nbsp;</span>
        <span>{{ info.height }} {{ info.weight }}LBS</span>
        <span class="text-blue-500">&nbsp;|&nbsp;</span>
        <span>Age {{ info.currentAge }}</span>
      </span>
    </div>
  </div>
</template>

<script>
export default {
  name: "InfoCard",
  props: {
    info: {
      type: Object,
      required: true,
    },
  },
};
</script>
