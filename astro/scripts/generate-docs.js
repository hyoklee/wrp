import { execSync } from 'child_process';
import { copyFileSync, mkdirSync, existsSync } from 'fs';
import { join } from 'path';

// Create docs directory if it doesn't exist
const docsDir = join(process.cwd(), 'src', 'pages', 'docs');
if (!existsSync(docsDir)) {
  mkdirSync(docsDir, { recursive: true });
}

// Run Doxygen
console.log('Generating Doxygen documentation...');
execSync('doxygen Doxyfile', { stdio: 'inherit' });

// Copy the generated HTML files to the Astro site
const doxygenOutputDir = join(process.cwd(), '..', 'docs', 'html');
const targetDir = join(process.cwd(), 'public', 'docs');

console.log('Copying Doxygen output to Astro site...');
if (!existsSync(targetDir)) {
  mkdirSync(targetDir, { recursive: true });
}

// Copy all files from doxygen output to public/docs
execSync(`cp -r "${doxygenOutputDir}"/* "${targetDir}/"`, { stdio: 'inherit' });

console.log('Documentation generation complete!'); 