const path = require('path');

module.exports = {
	title: 'Trevas',
	tagline: '',
	url: 'https://trevas.info',
	baseUrl: '/Trevas',
	onBrokenLinks: 'throw',
	onBrokenMarkdownLinks: 'warn',
	favicon: 'img/favicon.ico',
	organizationName: 'inseefr', // Usually your GitHub org/user name.
	projectName: 'trevas', // Usually your repo name.
	// Even if you don't use internalization, you can use this field to set useful
	// metadata like html lang. For example, if your site is Chinese, you may want
	// to replace "en" with "zh-Hans".
	i18n: {
		defaultLocale: 'en',
		locales: ['en', 'fr', 'zh-CN', 'no'],
	},
	customFields: {
		authors: {
			nicolas: {
				name: 'Nicolas Laval',
				link: 'https://github.com/NicoLaval',
				title: 'Making Sense - Developer',
				image: 'profile_pic_nicolas_laval.jpg',
			},
		},
	},
	themeConfig:
		/** @type {import('@docusaurus/preset-classic').ThemeConfig} */
		({
			// ...
			googleTagManager: {
				trackingID: 'insee',
			},
			prism: {
				additionalLanguages: [
					'csharp',
					'php',
					'ruby',
					'java',
					'rust',
					'elixir',
					'groovy',
					'sql',
				],
			},
			navbar: {
				style: 'dark',
				title: null,
				logo: {
					alt: 'Trevas logo',
					src: 'img/logo_trevas_short_dark.png',
				},
				// hideOnScroll: trues,
				items: [
					{
						type: 'search',
						position: 'right',
					},
					{
						to: '/introduction',
						activeBasePath: 'docs',
						label: 'Documentation',
						position: 'right',
					},
					{
						type: 'localeDropdown',
						position: 'right',
					},
					{
						href: 'https://github.com/InseeFr/Trevas',
						position: 'right',
						className: 'header-github-link',
						'aria-label': 'GitHub repository',
					},
				],
			},
			footer: {
				style: 'dark',
				copyright: ` `,
			},
			colorMode: {
				// Hides the switch in the navbar
				// Useful if you want to support a single color mode
				disableSwitch: false,
				defaultMode: 'dark',
				respectPrefersColorScheme: false,
			},
			//   announcementBar: {
			//     id: 'redisconf20201cfp', // Any value that will identify this message.
			//     content:
			//       '<a href="https://redis.com/redisdays/" target="_blank" rel="noopener">RedisDays Available Now On-Demand.</a>',
			//     backgroundColor: '#fff', // Defaults to `#fff`.
			//     textColor: '#000', // Defaults to `#000`.
			//     isCloseable: true, // Defaults to `true`.
			//   },
		}),
	presets: [
		[
			'classic',
			/** @type {import('@docusaurus/preset-classic').Options} */
			{
				docs: {
					routeBasePath: '/',
					path: 'docs',
					sidebarPath: require.resolve('./sidebars.js'),
					showLastUpdateTime: true,
				},
				blog: {
					showReadingTime: true,
				},
				theme: {
					customCss: require.resolve('./src/css/custom.scss'),
				},
				pages: {
					path: 'src/pages',
					routeBasePath: '/',
					include: ['**/*.{js,jsx,ts,tsx,md,mdx}'],
					exclude: [
						'**/_*.{js,jsx,ts,tsx,md,mdx}',
						'**/_*/**',
						'**/*.test.{js,jsx,ts,tsx}',
						'**/__tests__/**',
					],
					mdxPageComponent: '@theme/MDXPage',
					remarkPlugins: [],
					rehypePlugins: [],
					beforeDefaultRemarkPlugins: [],
					beforeDefaultRehypePlugins: [],
				},
				sitemap: {
					changefreq: 'weekly',
					priority: 0.5,
					ignorePatterns: ['/lp/**'],
					filename: 'sitemap.xml',
				},
			},
		],
	],
	plugins: [
		'docusaurus-plugin-sass',
		path.resolve(__dirname, 'plugins', 'gtm'),
		[
			require.resolve('docusaurus-lunr-search'),
			{
				indexBaseUrl: true,
				languages: ['en', 'fr', 'no'],
			},
		],
	],
	// GHA
	projectName: 'inseefr.github.io',
	organizationName: 'InseeFr',
	trailingSlash: false,
};
