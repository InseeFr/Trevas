import React from 'react';
import ReactPlayer from 'react-player/lazy';

interface VideoProps {
	url: string;
}

const Video = ({ url }: VideoProps) => (
	<ReactPlayer url={url} controls={true} width="100%" />
);

export default Video;
