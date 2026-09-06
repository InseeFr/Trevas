const tsParser = require('@typescript-eslint/parser');
const tsPlugin = require('@typescript-eslint/eslint-plugin');
const prettierPlugin = require('eslint-plugin-prettier');
const prettierConfig = require('eslint-config-prettier');
const simpleImportSort = require('eslint-plugin-simple-import-sort');

/** @type {import('eslint').Linter.Config[]} */
module.exports = [
	{
		ignores: [
			'test/**/*',
			'.prettierrc.js',
			'jest.config.js',
			'jest.setup.ts',
			'utils.d.ts',
			'utils.js',
			'.eslintrc.js',
			'eslint.config.js',
			'build/**',
			'dist/**',
			'.docusaurus/**',
			'node_modules/**',
			'docs/**',
			'.github/**',
			'static/**',
			'/*.js',
			'/*.ts',
			'/*.d.ts',
			// Vendored/search JS is outside tsconfig include.
			'src/theme/SearchBar/**',
		],
	},
	{
		files: ['src/**/*.{ts,tsx}'],
		languageOptions: {
			parser: tsParser,
			parserOptions: {
				tsconfigRootDir: __dirname,
				project: './tsconfig.json',
				ecmaFeatures: {
					jsx: true,
				},
				ecmaVersion: 2018,
				sourceType: 'module',
			},
		},
		plugins: {
			'@typescript-eslint': tsPlugin,
			prettier: prettierPlugin,
			'simple-import-sort': simpleImportSort,
		},
		rules: {
			...tsPlugin.configs['eslint-recommended'].rules,
			...tsPlugin.configs.recommended.rules,
			...tsPlugin.configs['recommended-requiring-type-checking'].rules,
			...prettierConfig.rules,
			'@typescript-eslint/unbound-method': 0,
			'@typescript-eslint/no-explicit-any': 0,
			'@typescript-eslint/no-require-imports': 0,
			'@typescript-eslint/no-var-requires': 0,
			'no-void': 0,
			'@typescript-eslint/no-unsafe-assignment': 0,
			'@typescript-eslint/no-unsafe-member-access': 0,
			'no-console': ['error', { allow: ['warn', 'error', 'debug'] }],
			'prettier/prettier': [
				'off',
				{
					endOfLine: 'crlf',
					useTabs: true,
				},
			],
		},
	},
];
