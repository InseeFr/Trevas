import useBaseUrl from '@docusaurus/useBaseUrl';
import React from 'react';

interface SasVTLProps {
	imgSas: string;
	imgVTL: string;
}

const SasVTL = ({ imgSas, imgVTL }: SasVTLProps) => (
	<div className="container">
		<div className="row">
			<div className="col col--6 centered-content">
				<h3>Sas</h3>
				<img src={useBaseUrl(`/img/sas-vtl/${imgSas}`)} alt="Img Sas" />
			</div>
			<div className="col col--6 centered-content">
				<h3>VTL</h3>
				<img src={useBaseUrl(`/img/sas-vtl/${imgVTL}`)} alt="Img VTL" />
			</div>
		</div>
	</div>
);

export default SasVTL;
