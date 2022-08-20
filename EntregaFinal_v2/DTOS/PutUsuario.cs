namespace Usuarios.DTOS
{
    //Intercambio de valores para MODIFICAR (PUT) un usuario
    public class PutUsuario
    {
        public int ID { get; set; }
        public string Nombre { get; set; }
        public string Apellido { get; set; }
        public string Contraseña { get; set; }
        public string NombreUsuario { get; set; }
        public string Mail { get; set; }
    }
}
