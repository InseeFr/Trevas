import React from 'react';
import useBaseUrl from '@docusaurus/useBaseUrl';
import { useColorMode } from '@docusaurus/theme-common';
import Translate from '@docusaurus/Translate';

import SvgArrowRight from '@site/src/svg/ArrowRight';
import SvgClientApps from '@site/src/svg/ClientApps';
import SvgDevelop from '@site/src/svg/Develop';
import SvgEngine from '@site/src/svg/Engine';
import SvgTrevasBox from '@site/src/svg/TrevasBox';

function Hero() {
	const { colorMode } = useColorMode();
	const isDarkTheme = colorMode === 'dark';
	const logoSrc = isDarkTheme
		? 'img/logo_trevas_dark.png'
		: 'img/logo_trevas_light.png';
	const svgColor = isDarkTheme ? 'white' : 'black';
	return (
		<header className="rds-hero">
			<div className="container">
				<div className="row">
					<div className="col col--12">
						<div className="row">
							<div className="col col--12 centered-content">
								<img src={logoSrc} alt="Trevas logo" />
							</div>
						</div>
						<div className="boxes">
							<div className="box">
								<span className="icon">
									<SvgTrevasBox color={svgColor} />
								</span>
								<div className="text">
									<h3 className="title">
										<Translate description="Box title Trevas">Trevas</Translate>
									</h3>
									<p className="description">
										<Translate description="Box description Trevas">
											Getting started with Trevas Java VTL engine
										</Translate>
									</p>
									<span className="more">
										<Translate description="See more">See more</Translate>{' '}
										<SvgArrowRight color="#DC382C" />
									</span>
								</div>
								<a href={useBaseUrl('/introduction')} className="link">
									<Translate description="See more">See more</Translate>
								</a>
							</div>

							<div className="box">
								<span className="icon">
									<SvgEngine color={svgColor} />
								</span>
								<div className="text">
									<h3 className="title">
										<Translate description="Box title coverage">
											Engine coverage
										</Translate>
									</h3>
									<p className="description">
										<Translate description="Box description coverage">
											Check the current coverage of VTL in Trevas engine
										</Translate>
									</p>
									<span className="more">
										<Translate description="See more">See more</Translate>{' '}
										<SvgArrowRight color="#DC382C" />
									</span>
								</div>
								<a href={useBaseUrl('/user-guide/coverage')} className="link">
									<Translate description="See more">See more</Translate>
								</a>
							</div>

							<div className="box">
								<span className="icon">
									<SvgDevelop color={svgColor} />
								</span>
								<div className="text">
									<h3 className="title">
										<Translate description="Box title VTL">
											VTL user guide
										</Translate>
									</h3>
									<p className="description">
										<Translate description="Box description VTL">
											Discover examples of VTL scripts
										</Translate>
									</p>
									<span className="more">
										<Translate description="See more">See more</Translate>{' '}
										<SvgArrowRight color="#DC382C" />
									</span>
								</div>
								<a href={useBaseUrl('/user-guide/vtl')} className="link">
									<Translate description="See more">See more</Translate>
								</a>
							</div>

							<div className="box">
								<span className="icon">
									<SvgClientApps color={svgColor} />
								</span>
								<div className="text">
									<h3 className="title">
										<Translate description="Box title client apps">
											Client apps
										</Translate>
									</h3>
									<p className="description">
										<Translate description="Box description client apps">
											Discover applications that embed Trevas
										</Translate>
									</p>
									<span className="more">
										<Translate description="See more">See more</Translate>{' '}
										<SvgArrowRight color="#DC382C" />
									</span>
								</div>
								<a
									href={useBaseUrl('/user-guide/vtl/client-apps')}
									className="link"
								>
									<Translate description="See more">See more</Translate>
								</a>
							</div>
						</div>
					</div>
				</div>
			</div>
		</header>
	);
}

export default Hero;
