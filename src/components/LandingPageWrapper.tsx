import Head from '@docusaurus/Head';
import React from 'react';
import useForm from '../hooks/useForm';
import useMunchkin from '../hooks/useMunchkin';
import useSqueeze from '../hooks/useSqueeze';

export interface LandingPageWrapperProps {
	hasForm?: boolean;
}

export default function LandingPageWrapper({
	hasForm = true,
	children,
}: React.PropsWithChildren<LandingPageWrapperProps>) {
	useSqueeze({ skip: !hasForm });
	useForm({ skip: !hasForm });
	useMunchkin({ skip: !hasForm });

	return (
		<>
			<Head>
				<meta name="robots" content="noindex, nofollow" />
				<meta name="description" content="Making Sense home page" />
				<meta name="author" content="SitePoint" />

				<meta property="og:title" content="Making Sense" />
				<meta property="og:type" content="website" />
				<meta property="og:url" content="https://making-sense.info" />
				<meta property="og:description" content="Making Sense home page" />
				<meta property="og:image" content="ms-logo.png" />
				{hasForm && (
					<script src="//lp.redis.com/js/forms2/js/forms2.min.js"></script>
				)}
				<script type="application/ld+json">
					{`
        "@context": "https://schema.org",
        "@type": "Corporation",
        "name": "Making Sense",
        "url": "http://making-sense.info",
        "iso6523Code": "0002914285739",
        "isicV4": "62",
        "logo": "",
        "sameAs": "https://github.com/Making-Sense-Info",
        "keywords": "metadata, knowledge graphs, semantics"
      `}
				</script>
			</Head>

			<main className="lp-main">{children}</main>
		</>
	);
}
