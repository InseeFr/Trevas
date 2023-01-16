import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import './index.scss';

interface CollaboratorsProps {
	frontMatter: {
		authors?: string[];
	};
}

function Collaborators({ frontMatter }: CollaboratorsProps): JSX.Element {
	const { siteConfig } = useDocusaurusContext();
	const collaboratorLookup: Record<
		string,
		{
			link: string;
			name: string;
			title: string;
			image: string;
			mail: string;
			github: string;
			linkedin: string;
		}
	> = (siteConfig.customFields as any).authors;

	return (
		<>
			{frontMatter.authors && (
				<div className="docAuthors">
					<hr />
					{frontMatter.authors.map((author) => {
						const { image, name, title, mail, github, linkedin } =
							collaboratorLookup[author];
						return (
							<div key={author} className="collaboratorByline">
								<img
									className="collaboratorProfileImage"
									src={useBaseUrl(
										`/img/${image ? image : 'default_author_profile_pic.png'}`
									)}
									alt={`Profile picture for ${name}`}
								/>
								<div>
									<div>
										<div>{title}</div>
									</div>
									<br />
									<div className="collaboratorContacts">
										{mail && (
											<div className="collaboratorContact">
												<a href={`mailto:${mail}`}>
													<img
														src={useBaseUrl('/img/mail.svg')}
														alt={`Mail ${name}`}
													/>
												</a>
											</div>
										)}
										{github && (
											<div className="collaboratorContact">
												<a
													href={github}
													target="_blank"
													rel="noreferrer noopener"
													aria-label={`${name} on Github`}
												>
													<img
														src={useBaseUrl('/img/github.svg')}
														alt={`Github ${name}`}
													/>
												</a>
											</div>
										)}
										{linkedin && (
											<div className="collaboratorContact">
												<a
													href={linkedin}
													target="_blank"
													rel="noreferrer noopener"
													aria-label={`${name} on LinkedIn`}
												>
													<img
														src={useBaseUrl('/img/linkedin.svg')}
														alt={`LinkedIn ${name}`}
													/>
												</a>
											</div>
										)}
									</div>
								</div>
							</div>
						);
					})}
					<hr />
				</div>
			)}
		</>
	);
}

export default Collaborators;
