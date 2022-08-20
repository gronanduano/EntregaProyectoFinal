namespace Usuarios.DTOS
{
    //Intercambio de valores para CREAR (POST) un usuario
    public class PostUsuario
    {
        public string Nombre { get; set; }
        public string Apellido { get; set; }
        public string Contraseña { get; set; }
        public string NombreUsuario { get; set; }
        public string Mail { get; set; }
    }
}
