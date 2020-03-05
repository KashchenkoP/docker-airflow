DROP TABLE IF EXISTS `default.h_companies_info`;

CREATE TABLE `default.h_companies_info` as (
    SELECT
        `hashed_id`,
        `src`,
        `src_id`,
        `country`,
        `city`,
        `stateregion`,
        `route`,
        `fulladdress`,
        `name`,
        `phone_number`,
        `website`,
        `email`
    from
        `default.h_companies_info_tmp`
    where
        `country` in ('afghanistan', 'american samoa', 'andorra', 'angola', 'anguilla', 'argentina', 'armenia', 'aruba', 'australia', 'azerbaijan', 'bahrain', 'bangladesh',
        'barbados', 'belize', 'bolivia', 'bosnia and herzegovina', 'brazil', 'brunei darussalam', 'bulgaria', 'burkina faso', 'burundi', 'cambodia', 'central african republic',
        'chile', 'china', 'colombia', 'comoros', 'congo', 'congo, the democratic republic of the', 'costa rica', 'cote divoire', 'cuba', 'curacao', 'dominica', 'dominican republic',
        'el salvador', 'equatorial guinea', 'eritrea', 'estonia', 'finland', 'gabon', 'georgia', 'ghana', 'greenland', 'grenada', 'guadeloupe', 'guam', 'guyana', 'iceland', 'iran',
        'ireland', 'jamaica', 'jersey', 'jordan', 'kenya', 'laos', 'latvia', 'lebanon', 'lesotho', 'liberia', 'lithuania', 'macau', 'malaysia', 'mali', 'martinique',
        'mauritania', 'micronesia, federated states of', 'mozambique', 'namibia', 'nepal', 'new zealand', 'niger', 'north korea', 'norway', 'oman', 'pakistan', 'palestine', 'poland',
        'portugal', 'qatar', 'romania', 'russia', 'russian federation', 'saint kitts and nevis', 'saint lucia', 'saint vincent and the grenadines', 'sao tome and principe', 'saudi arabia', 'senegal',
        'serbia', 'seychelles', 'sierra leone', 'singapore', 'sint maarten', 'solomon islands', 'south korea', 'sweden', 'switzerland', 'taiwan', 'thailand', 'tonga', 'tunisia',
        'turkey', 'turkmenistan', 'united arab emirates', 'united states', 'uruguay', 'uzbekistan', 'vanuatu', 'vatican city state (holy see)', 'vietnam', 'virgin islands (u.s.)',
        'wales', 'wallis and futuna islands', 'yemen', '', 'albania', 'algeria', 'antigua and barbuda', 'austria', 'bahamas', 'belarus', 'belgium', 'benin', 'bermuda', 'bhutan',
        'botswana', 'cameroon', 'canada', 'cape verde', 'cayman islands', 'chad', 'croatia', 'cyprus', 'czech republic', 'denmark', 'djibouti', 'east timor', 'ecuador', 'egypt',
        'ethiopia', 'fiji', 'france', 'french guiana', 'french polynesia', 'gambia', 'germany', 'gibraltar', 'greece', 'guatemala', 'guinea', 'guinea-bissau', 'haiti', 'honduras',
        'hong kong', 'hungary', 'india', 'indonesia', 'iraq', 'isle of man', 'israel', 'italy', 'japan', 'kazakhstan', 'kiribati', 'kuwait', 'kyrgyzstan', 'libya', 'liechtenstein',
        'luxembourg', 'macedonia', 'madagascar', 'malawi', 'maldives', 'malta', 'marshall islands', 'mauritius', 'mayotte', 'mexico', 'moldova, republic of', 'monaco', 'mongolia', 'montenegro',
        'montserrat', 'morocco', 'myanmar', 'netherlands', 'netherlands antilles', 'new caledonia', 'nicaragua', 'nigeria', 'panama', 'papua new guinea', 'paraguay', 'peru', 'philippines',
        'puerto rico', 'reunion', 'rwanda', 'saint barthelemy', 'saint martin', 'samoa (independent)', 'san marino', 'scotland', 'slovakia', 'slovenia', 'somalia', 'south africa', 'south sudan',
        'spain', 'sri lanka', 'sudan', 'suriname', 'swaziland', 'syria', 'tajikistan', 'tanzania', 'togo', 'trinidad and tobago', 'turks and caicos islands',
        'uganda', 'ukraine', 'united kingdom', 'venezuela', 'virgin islands (british)', 'zambia', 'zimbabwe')
);

