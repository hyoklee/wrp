import { defineConfig } from 'astro/config';
import tailwind from '@astrojs/tailwind';

export default defineConfig({
  integrations: [tailwind()],
  site: 'https://hyoklee.github.io',
  base: '/content-assimilation-engine'
}); 