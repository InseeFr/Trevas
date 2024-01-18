import useBaseUrl from '@docusaurus/useBaseUrl';
import React from 'react';

interface SasProps {
	imgSas: string;
}

const Sas = ({ imgSas }: SasProps) => (
	<div className="container">
		<div className="row">
			<div className="col col--3" />
			<div className="col col--6 centered-content">
				<h3>Sas</h3>
				<img src={useBaseUrl(`/img/sas-vtl/${imgSas}`)} alt="Img Sas" />
			</div>
		</div>
		<h3 className="centered-content">VTL</h3>
	</div>
);

export default Sas;
