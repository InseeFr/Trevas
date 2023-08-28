import React from 'react';

interface LinkProps {
	label: string;
	href: string;
}

const Link = ({ label, href }: LinkProps) => <a href={href}>{label}</a>;

export default Link;
