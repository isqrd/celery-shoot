import git from 'git-rev-sync';
import replace from 'rollup-plugin-replace';
import babel from 'rollup-plugin-babel';
import pkg from './package.json';

export default {
  input: 'src/index.js',
  external: Object.keys(pkg.dependencies).concat(['uuid/v4', 'os']),
  output: [
    {
      file: pkg.main,
      format: 'cjs',
      name: 'celery-shoot',
      sourcemap: false,
    },
    {
      file: pkg.module,
      format: 'es',
      name: 'celery-shoot',
      sourcemap: false,
    },
  ],
  plugins: [
    replace({
      npm_package_version: pkg.version,
      git_hash: git.short(),
    }),
    babel({
      exclude: 'node_modules/**',
      presets: [['@babel/env', {
        modules: false,
        targets: {
          node: true,
        },
      }]],
      babelrc: false,
    }),
  ],
};
