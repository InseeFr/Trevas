import React from 'react';
import Layout from '@theme/Layout';

import Hero from '@theme/Hero';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function Home() {
	const { siteConfig } = useDocusaurusContext();
	return (
		<Layout title={siteConfig.title} description={siteConfig.tagline}>
			<Hero />
			<main className="home-main"></main>
		</Layout>
	);
}

export default Home;
