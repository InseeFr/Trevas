/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import { useThemeConfig, type FooterLinkItem } from '@docusaurus/theme-common';
import useBaseUrl from '@docusaurus/useBaseUrl';

export interface FooterLinkProps {
	[key: string]: any;
	to: string;
	label: string;
	href: string;
	prependBaseUrlToHref: boolean;
}

function FooterLink({
	to,
	href,
	label,
	prependBaseUrlToHref,
	...props
}: FooterLinkProps) {
	const toUrl = useBaseUrl(to);
	const normalizedHref = useBaseUrl(href, {
		forcePrependBaseUrl: true,
	});
	return (
		<Link
			className="footer__link-item"
			{...(href
				? {
						target: '_blank',
						rel: 'noopener noreferrer',
						href: prependBaseUrlToHref ? normalizedHref : href,
				  }
				: {
						to: toUrl,
				  })}
			{...props}
		>
			{label}
		</Link>
	);
}

function Footer() {
	const { footer } = useThemeConfig();
	const { links = [] } = footer ?? {};

	if (!footer) {
		return null;
	}

	return (
		<footer
			className={clsx('footer', {
				'footer--dark': footer.style === 'dark',
			})}
		>
			<div className="container">
				<div className="row">
					<div className="col col--12 centered-content">
						<h3 className="sponsors-title">Sponsors</h3>
						<div className="sponsors">
							<a
								href="https://ec.europa.eu/eurostat/web/main/home"
								target="_blank"
								rel="noreferrer noopener"
							>
								<img
									src={useBaseUrl('/img/logo_eurostat.png')}
									alt="Logo Eurostat"
									width="48px"
									className="sponsor"
								/>
							</a>
							<a
								href="https://www.insee.fr"
								target="_blank"
								rel="noreferrer noopener"
							>
								<img
									src={useBaseUrl('/img/logo_insee.png')}
									alt="Logo Insee"
									width="32px"
									className="sponsor"
								/>
							</a>
							<a
								href="http://making-sense.info/"
								target="_blank"
								rel="noreferrer noopener"
							>
								<img
									src={useBaseUrl('/img/logo_ms.jpg')}
									alt="Logo Making Sense"
									width="36px"
									className="sponsor"
								/>
							</a>
							<a
								href="https://www.casd.eu/"
								target="_blank"
								rel="noreferrer noopener"
							>
								<img
									src={useBaseUrl('/img/logo_casd.png')}
									alt="Logo CASD"
									width="52px"
									height="auto"
									className="sponsor"
								/>
							</a>
						</div>
					</div>
				</div>
			</div>
		</footer>
	);
}

export default Footer;
