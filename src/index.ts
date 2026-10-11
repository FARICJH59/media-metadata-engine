export default {
  async fetch(request, env, ctx) {
    return new Response("Spatial Acoustic Pipeline is live!", {
      headers: { "content-type": "text/plain" },
    });
  },
};
