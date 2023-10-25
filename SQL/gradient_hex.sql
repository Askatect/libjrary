CREATE OR ALTER FUNCTION [dbo].[gradient_hex] (
	@target float, 
	@lower float, 
	@upper float,
	@hexmin char(7), 
	@hexmax char(7)
)
RETURNS char(7)
AS
BEGIN
	IF @lower = @upper
		RETURN @hexmin

	DECLARE @hexcode char(7) = (
		SELECT '#' + STRING_AGG(CONVERT(char(2), CONVERT(binary(1), [result], 1), 2), '')
		FROM (
			SELECT [rgb],
				[lower],
				[upper],
				CONVERT(int, [lower] + ([upper] - [lower])*(@target - @lower)/(@upper - @lower)) AS [result]
			FROM (
				SELECT [lower].[rgb],
					[lower].[value] AS [lower],
					[upper].[value] AS [upper]
				FROM [dbo].[hexcode_to_rgb](@hexmin) AS [lower]
					INNER JOIN [dbo].[hexcode_to_rgb](@hexmax) AS [upper]
						ON [lower].[rgb] = [upper].[rgb]
			) AS [T]
		) AS [T]
	)
	RETURN @hexcode
END
GO