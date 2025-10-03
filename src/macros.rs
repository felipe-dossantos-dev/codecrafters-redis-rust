/// Macro para analisar o nome de um comando e despachá-lo para o comando
/// e struct de parse corretos.
#[macro_export]
macro_rules! command_parser {
    (
        $command_name:expr, // O nome do comando (String/&str)
        $args:expr,         // O iterador de argumentos (mut args)
        $commands_vec:expr, // O vetor para adicionar o comando parseado (mut commands)
        $(
            $match_command_name:expr => (
                $command_enum:ident,
                $command_struct:ty
            )
        ),* $(,)?
    ) => {
        match $command_name {
            $(
                $match_command_name => {
                    // Note: Você precisará garantir que $args seja um &mut impl Iterator
                    // e que parse seja um método disponível para $command_struct
                    match <$command_struct>::parse(&mut $args) {
                        Ok(cmd) => {
                            $commands_vec.push(RedisCommand::$command_enum(cmd));
                        },
                        Err(e) => return Err(e), // Propaga o erro do parse
                    }
                }
            )*
            _ => {
                return Err("command not found".to_string());
            }
        }
    };
}
