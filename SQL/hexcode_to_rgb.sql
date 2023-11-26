CREATE OR ALTER FUNCTION [jra].[hexcode_to_rgb] (@hexcode char(7))
RETURNS @Output TABLE ([rgb] char(1), [value] int)
AS
BEGIN
	IF LEN(@hexcode) = 4
		SET @hexcode = '#' + SUBSTRING(@hexcode, 2, 1) + SUBSTRING(@hexcode, 2, 1) + SUBSTRING(@hexcode, 3, 1) + SUBSTRING(@hexcode, 3, 1) + SUBSTRING(@hexcode, 4, 1) + SUBSTRING(@hexcode, 4, 1)
	ELSE IF LEN(@hexcode) <> 7
		RETURN

	INSERT INTO @Output
	SELECT *
	FROM (
		VALUES ('r', CONVERT(int, CONVERT(binary(1), '0x' + SUBSTRING(@hexcode, 2, 2), 1))),
			('g', CONVERT(int, CONVERT(binary(1), '0x' + SUBSTRING(@hexcode, 4, 2), 1))),
			('b', CONVERT(int, CONVERT(binary(1), '0x' + SUBSTRING(@hexcode, 6, 2), 1)))
	) AS [Output]([rgb], [value])
	RETURN
END
GO