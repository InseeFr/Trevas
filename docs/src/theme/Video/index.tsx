import React from 'react';
import ReactPlayer from 'react-player';

interface VideoProps {
	url: string;
}

const Video = ({ url }: VideoProps) => (
	<ReactPlayer src={url} controls={true} width="100%" />
);

export default Video;
