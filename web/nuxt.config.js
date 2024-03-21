import axios from "axios";

const AWS = require("aws-sdk");
const bucket = "zavant-player-data";

async function getObject(key) {
  const s3 = new AWS.S3();
  const params = {
    Bucket: bucket,
    Key: key,
  };
  const data = await s3.getObject(params).promise();
  return data;
}

async function listBucketContents() {
  const s3 = new AWS.S3();
  let objects = [];
  let listParams = {
    Bucket: bucket,
    MaxKeys: 1000,
  };

  while (true) {
    let response = await s3.listObjectsV2(listParams).promise();
    objects = objects.concat(response.Contents);
    if (!response.IsTruncated) {
      break;
    }
    listParams.ContinuationToken = response.NextContinuationToken;
  }

  return objects.map((obj) => {
    const base = obj.Key.replace(".json", "");
    return {
      route: `/players/${base}`,
      payload: getObject(obj.Key),
    };
  });
}

export default {
  // Target: https://go.nuxtjs.dev/config-target
  target: "static",

  // Global page headers: https://go.nuxtjs.dev/config-head
  head: {
    title: "zavant-web",
    htmlAttrs: {
      lang: "en",
    },
    meta: [
      { charset: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { hid: "description", name: "description", content: "" },
      { name: "format-detection", content: "telephone=no" },
    ],
    link: [{ rel: "icon", type: "image/x-icon", href: "/favicon.ico" }],
  },

  // Global CSS: https://go.nuxtjs.dev/config-css
  css: [],

  // Plugins to run before rendering page: https://go.nuxtjs.dev/config-plugins
  plugins: [],

  // Auto import components: https://go.nuxtjs.dev/config-components
  components: true,

  // Modules for dev and build (recommended): https://go.nuxtjs.dev/config-modules
  buildModules: [
    // https://go.nuxtjs.dev/tailwindcss
    "@nuxtjs/tailwindcss",
  ],

  // Modules: https://go.nuxtjs.dev/config-modules
  modules: [
    // https://go.nuxtjs.dev/axios
    "@nuxtjs/axios",
  ],

  // Axios module configuration: https://go.nuxtjs.dev/config-axios
  axios: {
    // Workaround to avoid enforcing hard-coded localhost:3000: https://github.com/nuxt-community/axios-module/issues/308
    baseURL: "https://zavant-player-data.s3.amazonaws.com/",
  },

  // Build Configuration: https://go.nuxtjs.dev/config-build
  build: {},

  generate: {
    async routes() {
      const routes = await listBucketContents();
      return routes;
    },
  },
};
