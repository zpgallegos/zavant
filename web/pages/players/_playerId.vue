<template>
  <div class="m-4 p-4">
    <div class="max-w-md mb-4">
        <info-card :info="playerData.info" />
    </div>
    <div>
        <standard-batting :std-batting-data="playerData.std_batting" />
    </div>
  </div>
</template>

<script>
import InfoCard from "../../components/InfoCard.vue";
import StandardBatting from "../../components/StandardBatting.vue";

export default {
  name: "player",
  components: {
    StandardBatting,
    InfoCard,
  },

  

  async asyncData({ $axios, params }) {
    const { data } = await $axios.get(`${params.playerId}.json`);

    return { playerData: data };
  },
};
</script>
