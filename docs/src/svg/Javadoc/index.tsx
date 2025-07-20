import React from 'react';

const JavadocIcon = ({ color = 'black', ...rest }: any) => (
	<svg
		xmlns="http://www.w3.org/2000/svg"
		width="36"
		height="36"
		viewBox="0 0 24 24"
		{...rest}
	>
		<path
			fill={color}
			d="M9.4 16.6 4.8 12l4.6-4.6L8 6l-6 6 6 6zm5.2 0 4.6-4.6-4.6-4.6L16 6l6 6-6 6z"
		/>
	</svg>
);

export default JavadocIcon;
