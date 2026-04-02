import { defineConfig } from 'astro/config';
import vercel from '@astrojs/vercel/serverless';

export default defineConfig({
  site: 'https://matterunknown.com',
  output: 'server',
  adapter: vercel({
    webAnalytics: { enabled: true },
  }),
});
