import { defineConfig } from 'vite';
import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  server: {
    port: 3000,
    open: true,
    fs: {
      allow: ['..']
    },
    // Fix for WASM file loading - serve harness files with correct MIME type
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp'
    }
  },
  build: {
    outDir: 'dist',
    assetsDir: 'assets'
  },
  // Ensure WASM files are served with correct MIME type
  optimizeDeps: {
    exclude: ['@fs']
  }
});
