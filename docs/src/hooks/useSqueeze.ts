import useIsBrowser from '@docusaurus/useIsBrowser';
import { useLayoutEffect } from 'react';

const selectorsToRemove = [
	'#redis-sitesearch',
	'.navbar__link[href*="rediscloud"]',
	'.navbar__link[href*="launchpad"]',
	'footer.footer',
];

export interface SqueezeOptions {
	skip?: boolean;
}

export default function useSqueeze({ skip = false }: SqueezeOptions) {
	const isBrowser = useIsBrowser();
	useLayoutEffect(() => {
		if (skip || !isBrowser) {
			return;
		}

		try {
			selectorsToRemove.forEach((selector) => {
				document.querySelectorAll(selector).forEach((el) => {
					el.remove();
				});
			});

			const logoLink = document.querySelector('a.navbar__brand');

			if (!logoLink) {
				return;
			}

			logoLink.addEventListener('click', (ev) => {
				ev.preventDefault();
				ev.stopPropagation();
			});
		} catch (e) {}
	}, [skip, isBrowser]);
}
